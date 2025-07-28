/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.scheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Job index, id and jobInfo mapping.
 */
public class ScheduledJobInfo {
    private Map<String, Map<String, JobSchedulingInfo>> jobInfoMap;
    private Map<String, Map<String, JobSchedulingInfo>> disabledJobInfoMap;

    ScheduledJobInfo() {
        this.jobInfoMap = new ConcurrentHashMap<>();
        this.disabledJobInfoMap = new ConcurrentHashMap<>();
    }

    public Map<String, JobSchedulingInfo> getJobsByIndex(String indexName) {
        if (!this.jobInfoMap.containsKey(indexName)) {
            synchronized (this.jobInfoMap) {
                if (!this.jobInfoMap.containsKey(indexName)) {
                    this.jobInfoMap.put(indexName, new ConcurrentHashMap<>());
                }
            }
        }
        return this.jobInfoMap.get(indexName);
    }

    public Map<String, JobSchedulingInfo> getDisabledJobsByIndex(String indexName) {
        if (!this.disabledJobInfoMap.containsKey(indexName)) {
            synchronized (this.disabledJobInfoMap) {
                if (!this.disabledJobInfoMap.containsKey(indexName)) {
                    this.disabledJobInfoMap.put(indexName, new ConcurrentHashMap<>());
                }
            }
        }
        return this.disabledJobInfoMap.get(indexName);
    }

    public JobSchedulingInfo getJobInfo(String indexName, String jobId) {
        return getJobsByIndex(indexName).get(jobId);
    }

    public JobSchedulingInfo getDisabledJobInfo(String indexName, String jobId) {
        return getDisabledJobsByIndex(indexName).get(jobId);
    }

    public void addJob(String indexName, String jobId, JobSchedulingInfo jobInfo) {
        if (!this.jobInfoMap.containsKey(indexName)) {
            synchronized (this.jobInfoMap) {
                if (!this.jobInfoMap.containsKey(indexName)) {
                    jobInfoMap.put(indexName, new ConcurrentHashMap<>());
                }
            }
        }

        this.jobInfoMap.get(indexName).put(jobId, jobInfo);

        if (this.disabledJobInfoMap.containsKey(indexName)) {
            removeDisabledJob(indexName, jobId);
        }
    }

    public void addDisabledJob(String indexName, String jobId, JobSchedulingInfo jobInfo) {
        if (!this.disabledJobInfoMap.containsKey(indexName)) {
            synchronized (this.disabledJobInfoMap) {
                if (!this.disabledJobInfoMap.containsKey(indexName)) {
                    disabledJobInfoMap.put(indexName, new ConcurrentHashMap<>());
                }
            }
        }

        this.disabledJobInfoMap.get(indexName).put(jobId, jobInfo);
    }

    public Map<String, Map<String, JobSchedulingInfo>> getJobInfoMap() {
        return Map.copyOf(jobInfoMap);
    }

    public Map<String, Map<String, JobSchedulingInfo>> getDisabledJobInfoMap() {
        return Map.copyOf(disabledJobInfoMap);
    }

    public JobSchedulingInfo removeJob(String indexName, String jobId) {
        if (this.jobInfoMap.containsKey(indexName)) {
            return this.jobInfoMap.get(indexName).remove(jobId);
        }

        return null;
    }

    public JobSchedulingInfo removeDisabledJob(String indexName, String jobId) {
        if (this.disabledJobInfoMap.containsKey(indexName)) {
            return this.disabledJobInfoMap.get(indexName).remove(jobId);
        }

        return null;
    }

}
