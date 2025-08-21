/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.apache.lucene.tests.index.AssertingDirectoryReader;
import org.junit.Before;
import org.mockito.Mock;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.jobscheduler.rest.action.RestGetLocksAction;
import org.opensearch.jobscheduler.rest.action.RestGetJobDetailsAction;
import org.opensearch.jobscheduler.rest.action.RestGetLockAction;
import org.opensearch.jobscheduler.rest.action.RestGetScheduledInfoAction;
import org.opensearch.jobscheduler.rest.action.RestReleaseLockAction;
import org.opensearch.jobscheduler.rest.action.RestGetHistoryAction;
import org.opensearch.jobscheduler.spi.JobSchedulerExtension;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.transport.action.GetAllLocksAction;
import org.opensearch.jobscheduler.transport.action.GetScheduledInfoAction;
import org.opensearch.jobscheduler.transport.action.TransportGetAllLocksAction;
import org.opensearch.jobscheduler.transport.action.TransportGetScheduledInfoAction;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.plugins.ActionPlugin.ActionHandler;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import org.opensearch.test.engine.MockEngineFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobSchedulerPluginTests extends OpenSearchTestCase {

    private JobSchedulerPlugin plugin;

    private Index index;
    private Settings settings;
    private Settings sweeperSettings;
    private IndexSettings indexSettings;
    private IndexModule indexModule;
    private AnalysisRegistry emptyAnalysisRegistry;

    @Mock
    private RestController restController;
    @Mock
    private ClusterSettings clusterSettings;
    @Mock
    private IndexScopedSettings indexScopedSettings;
    @Mock
    private SettingsFilter settingsFilter;
    @Mock
    private IndexNameExpressionResolver indexNameExpressionResolver;
    @Mock
    private Supplier<DiscoveryNodes> nodesInCluster;

    @Before
    public void setup() {
        plugin = new JobSchedulerPlugin();
        settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        indexSettings = IndexSettingsModule.newIndexSettings(JobDetailsService.JOB_DETAILS_INDEX_NAME, settings);
        index = indexSettings.getIndex();
        final MockEngineFactory engineFactory = new MockEngineFactory(AssertingDirectoryReader.class);
        indexModule = new IndexModule(
            indexSettings,
            emptyAnalysisRegistry,
            engineFactory,
            new EngineConfigFactory(indexSettings),
            Collections.emptyMap(),
            () -> true,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            Collections.emptyMap()
        );
        sweeperSettings = Settings.builder()
            .put(JobSchedulerSettings.SWEEP_PERIOD.getKey(), TimeValue.timeValueMinutes(1))
            .put(JobSchedulerSettings.SWEEP_PAGE_SIZE.getKey(), 10)
            .put(JobSchedulerSettings.SWEEP_BACKOFF_MILLIS.getKey(), TimeValue.timeValueMillis(100))
            .put(JobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT.getKey(), 5)
            .build();
    }

    public void testLoadExtensions() {
        ExtensiblePlugin.ExtensionLoader mockLoader = mock(ExtensiblePlugin.ExtensionLoader.class);
        JobSchedulerExtension mockExtension1 = mock(JobSchedulerExtension.class);
        JobSchedulerExtension mockExtension2 = mock(JobSchedulerExtension.class);
        when(mockLoader.loadExtensions(JobSchedulerExtension.class)).thenReturn(Arrays.asList(mockExtension1, mockExtension2));
        when(mockExtension1.getJobType()).thenReturn("jobType1");
        when(mockExtension1.getJobIndex()).thenReturn("index1");
        when(mockExtension2.getJobType()).thenReturn("jobType2");
        when(mockExtension2.getJobIndex()).thenReturn("index2");
        ScheduledJobParser mockParser = mock(ScheduledJobParser.class);
        ScheduledJobRunner mockRunner = mock(ScheduledJobRunner.class);
        when(mockExtension1.getJobParser()).thenReturn(mockParser);
        when(mockExtension1.getJobRunner()).thenReturn(mockRunner);
        plugin.loadExtensions(mockLoader);
        assertEquals(2, plugin.getIndexToJobProviders().size());
        assertTrue(plugin.getIndicesToListen().contains("index1"));
        assertTrue(plugin.getIndicesToListen().contains("index2"));
    }

    public void testGetSettings_returnsSettingsList() {
        List<Setting<?>> settings = plugin.getSettings();
        assertNotNull(settings);
        assertEquals(13, settings.size());
        assertTrue(settings.contains(LegacyOpenDistroJobSchedulerSettings.SWEEP_PAGE_SIZE));
        assertTrue(settings.contains(LegacyOpenDistroJobSchedulerSettings.REQUEST_TIMEOUT));
        assertTrue(settings.contains(LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_MILLIS));
        assertTrue(settings.contains(LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT));
        assertTrue(settings.contains(LegacyOpenDistroJobSchedulerSettings.SWEEP_PERIOD));
        assertTrue(settings.contains(LegacyOpenDistroJobSchedulerSettings.JITTER_LIMIT));
        assertTrue(settings.contains(JobSchedulerSettings.SWEEP_PAGE_SIZE));
        assertTrue(settings.contains(JobSchedulerSettings.REQUEST_TIMEOUT));
        assertTrue(settings.contains(JobSchedulerSettings.SWEEP_BACKOFF_MILLIS));
        assertTrue(settings.contains(JobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT));
        assertTrue(settings.contains(JobSchedulerSettings.SWEEP_PERIOD));
        assertTrue(settings.contains(JobSchedulerSettings.JITTER_LIMIT));
    }

    public void testOnIndexModule() {
        assertEquals(indexModule.getIndex().toString(), index.toString());
        assertEquals(index.getName(), JobDetailsService.JOB_DETAILS_INDEX_NAME);
    }

    public void testGetRestHandlers() {
        List<RestHandler> restHandlers = plugin.getRestHandlers(
            settings,
            restController,
            clusterSettings,
            indexScopedSettings,
            settingsFilter,
            indexNameExpressionResolver,
            nodesInCluster
        );
        assertThat(
            restHandlers,
            containsInAnyOrder(
                instanceOf(RestGetJobDetailsAction.class),
                instanceOf(RestGetLockAction.class),
                instanceOf(RestReleaseLockAction.class),
                instanceOf(RestGetScheduledInfoAction.class),
                instanceOf(RestGetLocksAction.class),
                instanceOf(RestGetHistoryAction.class)
            )
        );
    }

    public void testGetIndicesToListen() {
        Set<String> expectedIndices = new HashSet<>();
        expectedIndices.add("index1");
        expectedIndices.add("index2");
        plugin.getIndicesToListen().addAll(expectedIndices);
        Set<String> actualIndices = plugin.getIndicesToListen();
        assertEquals(expectedIndices, actualIndices);
    }

    public void testGetSystemIndexDescriptors() {
        var descriptors = plugin.getSystemIndexDescriptors(settings);
        assertEquals(2, descriptors.size());
        var indexNames = descriptors.stream().map(d -> d.getIndexPattern()).toList();
        assertTrue(indexNames.contains(".opendistro-job-scheduler-lock"));
        assertTrue(indexNames.contains(".job-scheduler-history"));
    }

    public void testGetIndexToJobProviders() {
        Map<String, ScheduledJobProvider> expectedProviders = plugin.getIndexToJobProviders();
        ScheduledJobParser mockParser = mock(ScheduledJobParser.class);
        ScheduledJobRunner mockRunner = mock(ScheduledJobRunner.class);
        expectedProviders.put("index1", new ScheduledJobProvider("test-job-1", "test-job-index-1", mockParser, mockRunner));
        Map<String, ScheduledJobProvider> actualProviders = plugin.getIndexToJobProviders();
        assertEquals(expectedProviders, actualProviders);
    }

    public void testGetActions() {
        List<ActionHandler<?, ?>> actions = plugin.getActions();
        assertNotNull(actions);
        assertEquals(3, actions.size());
        ActionHandler<?, ?> actionHandler = actions.get(0);
        assertEquals(GetScheduledInfoAction.INSTANCE, actionHandler.getAction());
        assertEquals(TransportGetScheduledInfoAction.class, actionHandler.getTransportAction());
        ActionHandler<?, ?> actionHandler1 = actions.get(1);
        assertEquals(GetAllLocksAction.INSTANCE, actionHandler1.getAction());
        assertEquals(TransportGetAllLocksAction.class, actionHandler1.getTransportAction());
    }
}
