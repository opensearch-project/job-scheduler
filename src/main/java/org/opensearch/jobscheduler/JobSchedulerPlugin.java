/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler;

import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.identity.PluginSubject;
import org.opensearch.jobscheduler.rest.action.RestGetJobDetailsAction;
import org.opensearch.jobscheduler.rest.action.RestGetLockAction;
import org.opensearch.jobscheduler.rest.action.RestReleaseLockAction;
import org.opensearch.jobscheduler.scheduler.JobScheduler;
import org.opensearch.jobscheduler.spi.JobSchedulerExtension;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.sweeper.JobSweeper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
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
import org.opensearch.jobscheduler.transport.RunAsSubjectClient;
import org.opensearch.jobscheduler.utils.JobDetailsService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.IdentityAwarePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
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

import com.google.common.collect.ImmutableList;

public class JobSchedulerPlugin extends Plugin implements ActionPlugin, ExtensiblePlugin, SystemIndexPlugin, IdentityAwarePlugin {

    public static final String OPEN_DISTRO_JOB_SCHEDULER_THREAD_POOL_NAME = "open_distro_job_scheduler";
    public static final String JS_BASE_URI = "/_plugins/_job_scheduler";

    private static final Logger log = LogManager.getLogger(JobSchedulerPlugin.class);
    private JobSweeper sweeper;
    private JobScheduler scheduler;
    private LockService lockService;
    private Map<String, ScheduledJobProvider> indexToJobProviders;
    private Set<String> indicesToListen;
    private RunAsSubjectClient pluginClient;

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

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Collections.singletonList(
            new SystemIndexDescriptor(LockService.LOCK_INDEX_NAME, "Stores lock documents used for plugin job execution")
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
        this.pluginClient = new RunAsSubjectClient(client);
        this.lockService = new LockService(pluginClient, clusterService);
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

        return List.of(this.lockService);
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
        return ImmutableList.of(restGetJobDetailsAction, restGetLockAction, restReleaseLockAction);
    }

    @Override
    public void assignSubject(PluginSubject pluginSubject) {
        if (this.pluginClient != null) {
            this.pluginClient.setSubject(pluginSubject);
        }
    }

}
