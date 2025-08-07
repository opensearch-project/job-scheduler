/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.sampleextension;

import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.Timeout;
import org.junit.AfterClass;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.WarningsHandler;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.schedule.CronSchedule;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.secure_sm.AccessController;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class SampleExtensionIntegTestCase extends OpenSearchRestTestCase {

    @AfterClass
    public static void dumpCoverage() throws IOException, MalformedObjectNameException {
        // jacoco.dir is set in esplugin-coverage.gradle, if it doesn't exist we don't
        // want to collect coverage so we can return early
        String jacocoBuildPath = System.getProperty("jacoco.dir");
        if (org.opensearch.core.common.Strings.isNullOrEmpty(jacocoBuildPath)) {
            return;
        }

        String serverUrl = System.getProperty("jmx.serviceUrl");
        if (serverUrl == null) {
            // log.error("Failed to dump coverage because JMX Service URL is null");
            throw new IllegalArgumentException("JMX Service URL is null");
        }

        try (JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(serverUrl))) {
            IProxy proxy = MBeanServerInvocationHandler.newProxyInstance(
                connector.getMBeanServerConnection(),
                new ObjectName("org.jacoco:type=Runtime"),
                IProxy.class,
                false
            );

            Path path = PathUtils.get(jacocoBuildPath, "integTest.exec");
            AccessController.doPrivilegedChecked(() -> { Files.write(path, proxy.getExecutionData(false)); });
        } catch (Exception ex) {
            // log.error("Failed to dump coverage: ", ex);
            throw new RuntimeException("Failed to dump coverage: " + ex);
        }
    }

    /**
     * We need to be able to dump the jacoco coverage before cluster is shut down.
     * The new internal testing framework removed some of the gradle tasks we were listening to
     * to choose a good time to do it. This will dump the executionData to file after each test.
     * TODO: This is also currently just overwriting integTest.exec with the updated execData without
     * resetting after writing each time. This can be improved to either write an exec file per test
     * or by letting jacoco append to the file
     */
    public interface IProxy {
        byte[] getExecutionData(boolean reset);

        void dump(boolean reset);

        void reset();
    }

    protected boolean isHttps() {
        boolean isHttps = Optional.ofNullable(System.getProperty("https")).map("true"::equalsIgnoreCase).orElse(false);
        if (isHttps) {
            // currently only external cluster is supported for security enabled testing
            if (!Optional.ofNullable(System.getProperty("tests.rest.cluster")).isPresent()) {
                throw new RuntimeException("cluster url should be provided for security enabled testing");
            }
        }

        return isHttps;
    }

    @Override
    protected String getProtocol() {
        return isHttps() ? "https" : "http";
    }

    @Override
    protected Settings restAdminSettings() {
        return Settings.builder()
            .put("http.port", 9200)
            .put("plugins.security.ssl.http.enabled", isHttps())
            .put("plugins.security.ssl.http.pemcert_filepath", "sample.pem")
            .put("plugins.security.ssl.http.keystore_filepath", "test-kirk.jks")
            .put("plugins.security.ssl.http.keystore_password", "changeit")
            .build();
        // return Settings.builder().put("strictDeprecationMode", false).put("http.port", 9200).build();
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        boolean strictDeprecationMode = settings.getAsBoolean("strictDeprecationMode", true);
        RestClientBuilder builder = RestClient.builder(hosts);
        if (isHttps()) {
            String keystore = settings.get("plugins.security.ssl.http.keystore_filepath");
            if (Objects.nonNull(keystore)) {
                URI uri = null;
                try {
                    uri = this.getClass().getClassLoader().getResource("sample.pem").toURI();
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
                Path configPath = PathUtils.get(uri).getParent().toAbsolutePath();
                return new SecureRestClientBuilder(settings, configPath, hosts).build();
            } else {
                configureHttpsClient(builder, settings);
                builder.setStrictDeprecationMode(strictDeprecationMode);
                return builder.build();
            }
        } else {
            configureClient(builder, settings);
            builder.setStrictDeprecationMode(strictDeprecationMode);
            return builder.build();
        }

    }

    protected static void configureHttpsClient(RestClientBuilder builder, Settings settings) throws IOException {
        Map<String, String> headers = new HashMap<>(ThreadContext.buildDefaultHeaders(settings));
        String userName = Optional.ofNullable(System.getProperty("user")).orElseThrow(() -> new RuntimeException("user name is missing"));
        String password = Optional.ofNullable(System.getProperty("password"))
            .orElseThrow(() -> new RuntimeException("password is missing"));

        headers.put(
            "Authorization",
            "Basic " + Base64.getEncoder().encodeToString((userName + ":" + password).getBytes(StandardCharsets.UTF_8))
        );
        Header[] defaultHeaders = new Header[headers.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
        }
        builder.setDefaultHeaders(defaultHeaders);
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            try {
                final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                    .setSslContext(SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build())
                    // disable the certificate since our testing cluster just uses the default security configuration
                    .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    // See please https://issues.apache.org/jira/browse/HTTPCLIENT-2219
                    .setTlsDetailsFactory(new Factory<SSLEngine, TlsDetails>() {
                        @Override
                        public TlsDetails create(final SSLEngine sslEngine) {
                            return new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol());
                        }
                    })
                    .build();

                final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                    .setTlsStrategy(tlsStrategy)
                    .build();

                return httpClientBuilder.setConnectionManager(connectionManager);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        final TimeValue socketTimeout = TimeValue.parseTimeValue(
            socketTimeoutString == null ? "60s" : socketTimeoutString,
            CLIENT_SOCKET_TIMEOUT
        );
        builder.setRequestConfigCallback(
            conf -> conf.setResponseTimeout(Timeout.ofMilliseconds(Math.toIntExact(socketTimeout.getMillis())))
        );
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }

    protected SampleJobParameter createWatcherJob(String jobId, SampleJobParameter jobParameter) throws IOException {
        jobParameter.setEnabled(true);
        Map<String, String> params = getJobParameterAsMap(jobId, jobParameter);
        Response response = makeRequest(client(), "POST", SampleExtensionRestHandler.WATCH_INDEX_URI, params, null);
        Assert.assertEquals("Unable to create a watcher job", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();
        return getJobParameter(client(), responseJson.get("_id").toString());
    }

    protected void deleteWatcherJob(String jobId) throws IOException {
        Response response = makeRequest(
            client(),
            "DELETE",
            SampleExtensionRestHandler.WATCH_INDEX_URI,
            Collections.singletonMap("id", jobId),
            null
        );

        Assert.assertEquals("Unable to delete a watcher job", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    protected SampleJobParameter disableWatcherJob(String jobId, SampleJobParameter jobParameter) throws IOException {
        jobParameter.setEnabled(false);
        Map<String, String> params = getJobParameterAsMap(jobId, jobParameter);
        Response response = makeRequest(client(), "POST", SampleExtensionRestHandler.WATCH_INDEX_URI, params, null);
        Assert.assertEquals("Unable to create a watcher job", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();
        return getJobParameter(client(), responseJson.get("_id").toString());
    }

    protected SampleJobParameter enableWatcherJob(String jobId, SampleJobParameter jobParameter) throws IOException {
        jobParameter.setEnabled(true);
        Map<String, String> params = getJobParameterAsMap(jobId, jobParameter);
        Response response = makeRequest(client(), "POST", SampleExtensionRestHandler.WATCH_INDEX_URI, params, null);
        Assert.assertEquals("Unable to create a watcher job", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();
        return getJobParameter(client(), responseJson.get("_id").toString());
    }

    protected Response makeRequest(
        RestClient client,
        String method,
        String endpoint,
        Map<String, String> params,
        HttpEntity entity,
        Header... headers
    ) throws IOException {
        Request request = new Request(method, endpoint);
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.setWarningsHandler(WarningsHandler.PERMISSIVE);

        for (Header header : headers) {
            options.addHeader(header.getName(), header.getValue());
        }
        request.setOptions(options.build());
        request.addParameters(params);
        if (entity != null) {
            request.setEntity(entity);
        }
        return client.performRequest(request);
    }

    protected Map<String, String> getJobParameterAsMap(String jobId, SampleJobParameter jobParameter) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("id", jobId);
        params.put("job_name", jobParameter.getName());
        params.put("index", jobParameter.getIndexToWatch());
        params.put("enabled", String.valueOf(jobParameter.isEnabled()));
        if (jobParameter.getSchedule() instanceof IntervalSchedule) {
            params.put("interval", String.valueOf(((IntervalSchedule) jobParameter.getSchedule()).getInterval()));
        } else if (jobParameter.getSchedule() instanceof CronSchedule) {
            params.put("cron", ((CronSchedule) jobParameter.getSchedule()).getCronExpression());
        }
        params.put("lock_duration_seconds", String.valueOf(jobParameter.getLockDurationSeconds()));
        return params;
    }

    @SuppressWarnings("unchecked")
    protected SampleJobParameter getJobParameter(RestClient client, String jobId) throws IOException {
        Request request = new Request("POST", "/" + SampleExtensionPlugin.JOB_INDEX_NAME + "/_search");
        String entity = """
            {
                "query": {
                    "match": {
                        "_id": {
                            "query": "%s"
                        }
                    }
                }
            }
            """.formatted(jobId);
        request.setJsonEntity(entity);
        Response response = client.performRequest(request);
        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();
        Map<String, Object> hit = (Map<String, Object>) ((List<Object>) ((Map<String, Object>) responseJson.get("hits")).get("hits")).get(
            0
        );
        Map<String, Object> jobSource = (Map<String, Object>) hit.get("_source");

        SampleJobParameter jobParameter = new SampleJobParameter();
        jobParameter.setJobName(jobSource.get("name").toString());
        jobParameter.setIndexToWatch(jobSource.get("index_name_to_watch").toString());

        Map<String, Object> jobSchedule = (Map<String, Object>) jobSource.get("schedule");
        if (jobSchedule.containsKey("cron")) {
            jobParameter.setSchedule(
                new CronSchedule(
                    ((Map<String, Object>) jobSchedule.get("cron")).get("expression").toString(),
                    ZoneId.of(((Map<String, Object>) jobSchedule.get("cron")).get("timezone").toString())
                )
            );
        } else {
            jobParameter.setSchedule(
                new IntervalSchedule(
                    Instant.ofEpochMilli(Long.parseLong(((Map<String, Object>) jobSchedule.get("interval")).get("start_time").toString())),
                    Integer.parseInt(((Map<String, Object>) jobSchedule.get("interval")).get("period").toString()),
                    ChronoUnit.SECONDS
                )

            );
        }
        jobParameter.setLockDurationSeconds(Long.parseLong(jobSource.get("lock_duration_seconds").toString()));
        jobParameter.setEnabled(Boolean.parseBoolean(jobSource.get("enabled").toString()));
        return jobParameter;
    }

    protected String createTestIndex() throws IOException {
        String index = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createTestIndex(index);
        return index;
    }

    protected void createTestIndex(String index) throws IOException {
        createIndex(index, Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
    }

    protected void deleteTestIndex(String index) throws IOException {
        deleteIndex(index);
    }

    protected int countRecordsInTestIndex(String index) throws IOException {
        String entity = """
            {
                "query": {
                    "match_all": {
                    }
                }
            }
            """;
        Response response = makeRequest(
            client(),
            "POST",
            "/" + index + "/_count",
            Collections.emptyMap(),
            new StringEntity(entity, ContentType.APPLICATION_JSON)
        );
        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();
        return Integer.parseInt(responseJson.get("count").toString());
    }

    protected void waitUntilLockIsAcquiredAndReleased(String jobId) {
        waitUntilLockIsAcquiredAndReleased(jobId, 20);
    }

    protected void waitUntilLockIsAcquiredAndReleased(String jobId, int maxTimeInSec) {
        AtomicLong prevLockAcquiredTime = new AtomicLong(0L);
        AtomicReference<LockModel> lock = new AtomicReference<>();
        try {
            lock.set(getLockByJobId(jobId));
            if (lock.get() != null && prevLockAcquiredTime.get() == 0L && lock.get().isReleased()) {
                prevLockAcquiredTime.set(lock.get().getLockTime().toEpochMilli());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        await().atMost(maxTimeInSec, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).ignoreExceptions().until(() -> {
            lock.set(getLockByJobId(jobId));
            return lock.get() != null && lock.get().getLockTime().toEpochMilli() != prevLockAcquiredTime.get() && lock.get().isReleased();
        });
    }

    @SuppressWarnings("unchecked")
    protected LockModel getLockByJobId(String jobId) throws IOException {
        String entity = """
            {
                "query": {
                    "match": {
                        "job_id": {
                            "query": "%s"
                        }
                    }
                }
            }
            """.formatted(jobId);
        Response response = makeRequest(
            client(),
            "POST",
            "/.opendistro-job-scheduler-lock/_search",
            Map.of("ignore", "404"),
            new StringEntity(entity, ContentType.APPLICATION_JSON)
        );
        if (response.getStatusLine().getStatusCode() == 404) {
            return null;
        }
        Map<String, Object> responseJson = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).map();
        List<Map<String, Object>> hits = (List<Map<String, Object>>) ((Map<String, Object>) responseJson.get("hits")).get("hits");
        if (hits.isEmpty()) {
            return null;
        }
        Map<String, Object> lockSource = (Map<String, Object>) hits.get(0).get("_source");
        return new LockModel(
            lockSource.get(LockModel.JOB_INDEX_NAME).toString(),
            lockSource.get(LockModel.JOB_ID).toString(),
            Instant.ofEpochMilli(Long.parseLong(lockSource.get(LockModel.LOCK_TIME).toString())),
            Long.parseLong(lockSource.get(LockModel.LOCK_DURATION).toString()),
            Boolean.parseBoolean(lockSource.get(LockModel.RELEASED).toString())
        );
    }
}
