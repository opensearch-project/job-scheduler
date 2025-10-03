/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.spi;

import org.opensearch.plugins.Plugin;

/**
 * OpenSearch Job Scheduler SPI Plugin
 * 
 * This plugin provides the Service Provider Interface (SPI) for the OpenSearch Job Scheduler.
 * It contains the interfaces and utilities needed for other plugins to extend the job scheduler functionality.
 */
public class JobSchedulerSpiPlugin extends Plugin {

    /**
     * Plugin constructor
     */
    public JobSchedulerSpiPlugin() {
        // Plugin initialization
    }

    @Override
    public String toString() {
        return "JobSchedulerSpiPlugin";
    }
}
