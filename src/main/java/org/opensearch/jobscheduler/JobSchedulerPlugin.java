/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.inject.Module;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.identity.PluginSubject;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.jobscheduler.rest.action.RestGetLocksAction;
import org.opensearch.jobscheduler.rest.action.RestGetJobDetailsAction;
import org.opensearch.jobscheduler.rest.action.RestGetLockAction;
import org.opensearch.jobscheduler.rest.action.RestGetScheduledInfoAction;
import org.opensearch.jobscheduler.rest.action.RestReleaseLockAction;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.transport.PluginClient;
import org.opensearch.jobscheduler.transport.action.GetAllLocksAction;
import org.opensearch.jobscheduler.transport.action.GetScheduledInfoAction;
import org.opensearch.jobscheduler.transport.action.TransportGetAllLocksAction;
import org.opensearch.jobscheduler.transport.action.TransportGetScheduledInfoAction;
import org.opensearch.jobscheduler.scheduler.JobScheduler;
import org.opensearch.jobscheduler.spi.JobSchedulerExtension;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser;
import org.opensearch.jobscheduler.utils.LockServiceImpl;
import org.opensearch.jobscheduler.sweeper.JobSweeper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.jobscheduler.utils.JobHistoryService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.IdentityAwarePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.impl.SdkClientFactory;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.function.Supplier;

import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_ENDPOINT_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_REGION_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_SERVICE_NAME_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_TYPE_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_AWARE_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_ID_FIELD_KEY;

public class JobSchedulerPlugin extends Plugin implements ActionPlugin, ExtensiblePlugin, SystemIndexPlugin, IdentityAwarePlugin {

    public static final String OPEN_DISTRO_JOB_SCHEDULER_THREAD_POOL_NAME = "open_distro_job_scheduler";
    public static final String JS_BASE_URI = "/_plugins/_job_scheduler";
    public static final String TENANT_ID_FIELD = "tenant_id";

    private static final Logger log = LogManager.getLogger(JobSchedulerPlugin.class);
    private JobSweeper sweeper;
    private JobScheduler scheduler;
    private LockService lockService;
    private JobHistoryService historyService;
    private Map<String, ScheduledJobProvider> indexToJobProviders;
    private Set<String> indicesToListen;
    private PluginClient pluginClient;
    private SdkClient sdkClient;

    private JobDetailsService jobDetailsService;

    public JobSchedulerPlugin() {
        this.indicesToListen = new HashSet<>();
        this.indexToJobProviders = new HashMap<>();
    }

    public Set<String> getIndicesToListen() {
        return indicesToListen;
    }

    public Map<String, ScheduledJobProvider> getIndexToJobProviders() {
        return indexToJobProviders;
    }

    public SdkClient getSdkClient() {
        return sdkClient;
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(new JobSchedulerPluginModule());
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            new SystemIndexDescriptor(LockServiceImpl.LOCK_INDEX_NAME, "Stores lock documents used for plugin job execution"),
            new SystemIndexDescriptor(JobHistoryService.JOB_HISTORY_INDEX_NAME, "Stores history documents used for plugin job execution")
        );
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        Settings settings = environment.settings();
        this.pluginClient = new PluginClient(client);

        // Initialize SDK client for remote metadata storage
        this.sdkClient = SdkClientFactory.createSdkClient(
            pluginClient,
            xContentRegistry,
            JobSchedulerSettings.JOB_SCHEDULER_MULTI_TENANCY_ENABLED.get(settings)
                ? Map.ofEntries(
                    Map.entry(REMOTE_METADATA_TYPE_KEY, JobSchedulerSettings.REMOTE_METADATA_TYPE.get(settings)),
                    Map.entry(REMOTE_METADATA_ENDPOINT_KEY, JobSchedulerSettings.REMOTE_METADATA_ENDPOINT.get(settings)),
                    Map.entry(REMOTE_METADATA_REGION_KEY, JobSchedulerSettings.REMOTE_METADATA_REGION.get(settings)),
                    Map.entry(REMOTE_METADATA_SERVICE_NAME_KEY, JobSchedulerSettings.REMOTE_METADATA_SERVICE_NAME.get(settings)),
                    Map.entry(TENANT_AWARE_KEY, "true"),
                    Map.entry(TENANT_ID_FIELD_KEY, TENANT_ID_FIELD)
                )
                : Collections.emptyMap()
        );

        Supplier<Boolean> statusHistoryEnabled = () -> JobSchedulerSettings.STATUS_HISTORY.get(environment.settings());
        this.historyService = new JobHistoryService(pluginClient, clusterService);
        this.lockService = new LockServiceImpl(pluginClient, clusterService, historyService, statusHistoryEnabled, this.sdkClient);
        this.jobDetailsService = new JobDetailsService(client, clusterService, this.indicesToListen, this.indexToJobProviders);
        this.scheduler = new JobScheduler(threadPool, this.lockService);
        this.sweeper = initSweeper(
            environment.settings(),
            client,
            clusterService,
            threadPool,
            xContentRegistry,
            this.scheduler,
            this.lockService,
            this.jobDetailsService
        );
        clusterService.addListener(this.sweeper);
        clusterService.addLifecycleListener(this.sweeper);

        return List.of(this.lockService, this.scheduler, this.jobDetailsService, this.pluginClient, this.sdkClient);
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingList = new ArrayList<>();
        settingList.add(LegacyOpenDistroJobSchedulerSettings.SWEEP_PAGE_SIZE);
        settingList.add(LegacyOpenDistroJobSchedulerSettings.REQUEST_TIMEOUT);
        settingList.add(LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_MILLIS);
        settingList.add(LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT);
        settingList.add(LegacyOpenDistroJobSchedulerSettings.SWEEP_PERIOD);
        settingList.add(LegacyOpenDistroJobSchedulerSettings.JITTER_LIMIT);
        settingList.add(JobSchedulerSettings.SWEEP_PAGE_SIZE);
        settingList.add(JobSchedulerSettings.REQUEST_TIMEOUT);
        settingList.add(JobSchedulerSettings.SWEEP_BACKOFF_MILLIS);
        settingList.add(JobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT);
        settingList.add(JobSchedulerSettings.SWEEP_PERIOD);
        settingList.add(JobSchedulerSettings.JITTER_LIMIT);
        settingList.add(JobSchedulerSettings.STATUS_HISTORY);
        settingList.add(JobSchedulerSettings.REMOTE_METADATA_TYPE);
        settingList.add(JobSchedulerSettings.REMOTE_METADATA_ENDPOINT);
        settingList.add(JobSchedulerSettings.REMOTE_METADATA_REGION);
        settingList.add(JobSchedulerSettings.REMOTE_METADATA_SERVICE_NAME);
        settingList.add(JobSchedulerSettings.JOB_SCHEDULER_MULTI_TENANCY_ENABLED);
        return settingList;
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final int processorCount = OpenSearchExecutors.allocatedProcessors(settings);

        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(
            new FixedExecutorBuilder(
                settings,
                OPEN_DISTRO_JOB_SCHEDULER_THREAD_POOL_NAME,
                processorCount,
                200,
                "opendistro.jobscheduler.threadpool"
            )
        );

        return executorBuilders;
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (indexModule.getIndex().getName().equals(JobDetailsService.JOB_DETAILS_INDEX_NAME)) {
            indexModule.addIndexOperationListener(this.jobDetailsService);
            log.info("JobDetailsService started listening to operations on index {}", JobDetailsService.JOB_DETAILS_INDEX_NAME);
        }
        if (this.indicesToListen.contains(indexModule.getIndex().getName())) {
            indexModule.addIndexOperationListener(this.sweeper);
            log.info("JobSweeper started listening to operations on index {}", indexModule.getIndex().getName());
        }
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {

        for (JobSchedulerExtension extension : loader.loadExtensions(JobSchedulerExtension.class)) {
            String jobType = extension.getJobType();
            String jobIndexName = extension.getJobIndex();
            ScheduledJobParser jobParser = extension.getJobParser();
            ScheduledJobRunner runner = extension.getJobRunner();
            if (this.indexToJobProviders.containsKey(jobIndexName)) {
                continue;
            }

            ScheduledJobProvider provider = new ScheduledJobProvider(jobType, jobIndexName, jobParser, runner);
            this.indexToJobProviders.put(jobIndexName, provider);
            this.indicesToListen.add(jobIndexName);
            log.info("Loaded scheduler extension: {}, index: {}", jobType, jobIndexName);
        }
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        List<NamedXContentRegistry.Entry> registryEntries = new ArrayList<>();

        // register schedule
        NamedXContentRegistry.Entry scheduleEntry = new NamedXContentRegistry.Entry(
            Schedule.class,
            new ParseField("schedule"),
            ScheduleParser::parse
        );
        registryEntries.add(scheduleEntry);

        return registryEntries;
    }

    private JobSweeper initSweeper(
        Settings settings,
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        NamedXContentRegistry registry,
        JobScheduler scheduler,
        LockService lockService,
        JobDetailsService jobDetailsService
    ) {
        return new JobSweeper(
            settings,
            client,
            clusterService,
            threadPool,
            registry,
            this.indexToJobProviders,
            scheduler,
            lockService,
            jobDetailsService
        );
    }

    @Override
    public List getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        RestGetJobDetailsAction restGetJobDetailsAction = new RestGetJobDetailsAction(jobDetailsService);
        RestGetLockAction restGetLockAction = new RestGetLockAction(lockService);
        RestReleaseLockAction restReleaseLockAction = new RestReleaseLockAction(lockService);
        RestGetScheduledInfoAction restGetScheduledInfoAction = new RestGetScheduledInfoAction();
        RestGetLocksAction restGetAllLocksAction = new RestGetLocksAction();
        return List.of(
            restGetJobDetailsAction,
            restGetLockAction,
            restReleaseLockAction,
            restGetScheduledInfoAction,
            restGetAllLocksAction
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>(2);
        actions.add(new ActionHandler<>(GetScheduledInfoAction.INSTANCE, TransportGetScheduledInfoAction.class));
        actions.add(new ActionHandler<>(GetAllLocksAction.INSTANCE, TransportGetAllLocksAction.class));
        return actions;
    }

    @Override
    public void assignSubject(PluginSubject pluginSubject) {
        // When security is not installed, the pluginSubject will still be assigned.
        assert pluginSubject != null;
        this.pluginClient.setSubject(pluginSubject);
    }

}
