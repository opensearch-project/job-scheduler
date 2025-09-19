/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler;

import org.opensearch.common.inject.AbstractModule;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.utils.LockServiceImpl;

/**
 * Guice Module to manage JobScheduler related objects
 */
public class JobSchedulerPluginModule extends AbstractModule {

    /**
     * Constructor for JobSchedulerPluginModule
     */
    public JobSchedulerPluginModule() {}

    @Override
    protected void configure() {
        bind(LockService.class).to(LockServiceImpl.class);
    }
}
