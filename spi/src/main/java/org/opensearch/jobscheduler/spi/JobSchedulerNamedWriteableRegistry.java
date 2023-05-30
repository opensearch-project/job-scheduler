/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.spi;

import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.BasicAuthToken;
import org.opensearch.identity.tokens.BearerAuthToken;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class JobSchedulerNamedWriteableRegistry {

    public static NamedWriteableRegistry registry = null;

    public static NamedWriteableRegistry getNamedWriteableRegistry() {
        if (registry == null) {
            registry = new NamedWriteableRegistry(
                Stream.of(
                    List.of(
                        new NamedWriteableRegistry.Entry(AuthToken.class, BasicAuthToken.NAME, BasicAuthToken::new),
                        new NamedWriteableRegistry.Entry(AuthToken.class, BearerAuthToken.NAME, BearerAuthToken::new)
                    ).stream()
                ).flatMap(Function.identity()).collect(toList())
            );
        }
        return registry;
    }
}
