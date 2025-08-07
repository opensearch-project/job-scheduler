/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.spi.utils;

import org.opensearch.core.action.ActionListener;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;

public interface LockService {

    /**
     * Attempts to acquire lock the job. If the lock does not exists it attempts to create the lock document.
     * If the Lock document exists, it will try to update and acquire lock.
     *
     * @param jobParameter a {@code ScheduledJobParameter} containing the lock duration.
     * @param context a {@code JobExecutionContext} containing job index name and job id.
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the lock if it was acquired
     *                 or else null. Passes {@code IllegalArgumentException} to onFailure if the {@code ScheduledJobParameter} does not
     *                 have {@code LockDurationSeconds}.
     */
    void acquireLock(final ScheduledJobParameter jobParameter, final JobExecutionContext context, ActionListener<LockModel> listener);

    /**
     * Attempts to acquire a lock with a specific lock Id. If the lock does not exist it attempts to create the lock document.
     * If the Lock document exists, it will try to update and acquire the lock.
     *
     * @param jobIndexName a non-null job index name.
     * @param lockDurationSeconds the amount of time in seconds that the lock should exist
     * @param lockId the unique Id for the lock. This should represent the resource that the lock is on, whether it be
     *               a job, or some other arbitrary resource. If the lockID matches a jobID, then the lock will be deleted
     *               when the job is deleted.
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return the lock if it was acquired
     *                 or else null. Passes {@code IllegalArgumentException} to onFailure if the {@code ScheduledJobParameter} does not
     *                 have {@code LockDurationSeconds}.
     */
    void acquireLockWithId(
        final String jobIndexName,
        final Long lockDurationSeconds,
        final String lockId,
        ActionListener<LockModel> listener
    );

    void findLock(final String lockId, ActionListener<LockModel> listener);

    /**
     * Attempt to release the lock.
     * Most failure cases are due to {@code lock.seqNo} and {@code lock.primaryTerm} not matching with the existing
     * document.
     *
     * @param lock a {@code LockModel} to be released.
     * @param listener a {@code ActionListener} that has onResponse and onFailure that is used to return whether
     *                 or not the release was successful
     */
    void release(final LockModel lock, ActionListener<Boolean> listener);

    /**
     * Attempt to delete lock.
     * This should be called as part of clean up when the job for corresponding lock is deleted.
     *
     * @param lockId a {@code String} to be deleted.
     * @param listener an {@code ActionListener} that has onResponse and onFailure that is used to return whether
     *                 or not the delete was successful
     */
    void deleteLock(final String lockId, ActionListener<Boolean> listener);

    /**
     * Attempt to renew a lock.
     * It is used to give an extended valid period to a lock. The start time of the lock will be updated to
     * the current time when the method get called, and the duration of the lock remains.
     * It works as long as the lock is not acquired by others, and no matter if the lock is expired of not.
     *
     * @param lock a {@code LockModel} to be renewed.
     * @param listener a {@code ActionListener} that has onResponse and onFailure that is used to
     *                 return the renewed lock if renewal succeed, otherwise return null.
     */
    void renewLock(final LockModel lock, ActionListener<LockModel> listener);
}
