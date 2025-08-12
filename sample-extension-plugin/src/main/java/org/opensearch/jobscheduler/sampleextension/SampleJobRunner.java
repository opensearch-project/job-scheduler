/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.sampleextension;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.plugins.Plugin;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.util.List;
import java.util.UUID;

/**
 * A sample job runner class.
 *
 * The job runner should be a singleton class if it uses OpenSearch client or other objects passed
 * from OpenSearch. Because when registering the job runner to JobScheduler plugin, OpenSearch has
 * not invoke plugins' createComponents() method. That is saying the plugin is not completely initalized,
 * and the OpenSearch {@link Client}, {@link ClusterService} and other objects
 * are not available to plugin and this job runner.
 *
 * So we have to move this job runner intialization to {@link Plugin} createComponents() method, and using
 * singleton job runner to ensure we register a usable job runner instance to JobScheduler plugin.
 *
 * This sample job runner takes the "indexToWatch" from job parameter and logs that index's shards.
 */
public class SampleJobRunner implements ScheduledJobRunner {

    private static final Logger log = LogManager.getLogger(ScheduledJobRunner.class);

    private static SampleJobRunner INSTANCE;

    public static SampleJobRunner getJobRunnerInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (SampleJobRunner.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new SampleJobRunner();
            return INSTANCE;
        }
    }

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;

    private SampleJobRunner() {
        // Singleton class, use getJobRunner method instead of constructor
    }

    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public void setClient(Client client) {
        this.client = client;
    }

    @Override
    public void runJob(ScheduledJobParameter jobParameter, JobExecutionContext context) {
        log.info("Starting job: {}, lockDurationSeconds: {}", jobParameter.getName(), jobParameter.getLockDurationSeconds());
        
        if (!(jobParameter instanceof SampleJobParameter)) {
            throw new IllegalStateException(
                "Job parameter is not instance of SampleJobParameter, type: " + jobParameter.getClass().getCanonicalName()
            );
        }

        if (this.clusterService == null) {
            throw new IllegalStateException("ClusterService is not initialized.");
        }

        if (this.threadPool == null) {
            throw new IllegalStateException("ThreadPool is not initialized.");
        }

        final LockService lockService = context.getLockService();

        if (jobParameter.getLockDurationSeconds() != null) {
            lockService.acquireLock(jobParameter, context, ActionListener.wrap(lock -> {
                if (lock == null) {
                    context.setJobStatus(-2);
                    return;
                }

                SampleJobParameter parameter = (SampleJobParameter) jobParameter;
                StringBuilder msg = new StringBuilder();
                msg.append("Watching index ").append(parameter.getIndexToWatch()).append("\n");

                List<ShardRouting> shardRoutingList = this.clusterService.state().routingTable().allShards(parameter.getIndexToWatch());
                for (ShardRouting shardRouting : shardRoutingList) {
                    msg.append(shardRouting.shardId().getId())
                        .append("\t")
                        .append(shardRouting.currentNodeId())
                        .append("\t")
                        .append(shardRouting.active() ? "active" : "inactive")
                        .append("\n");
                }
                log.info(msg.toString());
                runTaskForLockIntegrationTests(parameter);
                runTaskForIntegrationTests(parameter, ActionListener.wrap(idxResponse -> {
                    lockService.release(lock, ActionListener.wrap(released -> {
                        context.setJobStatus(0);
                        log.info("Released lock for job {}, setting status to 0", jobParameter.getName());
                    }, exception -> { context.setJobStatus(-2); throw new IllegalStateException("Failed to release lock."); }));
                }, exception -> { context.setJobStatus(-2); throw new IllegalStateException("Failed to index sample doc."); }));
            }, exception -> { context.setJobStatus(-2); throw new IllegalStateException("Failed to acquire lock."); }));
        }
    }

    private void runTaskForIntegrationTests(SampleJobParameter jobParameter, ActionListener<IndexResponse> listener) {
        this.client.index(
            new IndexRequest(jobParameter.getIndexToWatch()).id(UUID.randomUUID().toString())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source("{\"message\": \"message\"}", XContentType.JSON),
            listener
        );
    }

    private void runTaskForLockIntegrationTests(SampleJobParameter jobParameter) throws InterruptedException {
        if (jobParameter.getName().equals("sample-job-lock-test-it")) {
            Thread.sleep(10000);
        }
    }
}
