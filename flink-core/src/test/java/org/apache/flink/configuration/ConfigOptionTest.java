/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.configuration;

import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;

/** Tests for the {@link ConfigOption}. */
public class ConfigOptionTest extends TestLogger {

    @Test
    void testDeprecationFlagForDeprecatedKeys() {
        final ConfigOption<Integer> optionWithDeprecatedKeys =
                ConfigOptions.key("key")
                        .intType()
                        .defaultValue(0)
                        .withDeprecatedKeys("deprecated1", "deprecated2");

        Assertions.assertTrue(optionWithDeprecatedKeys.hasFallbackKeys());
        for (final FallbackKey fallbackKey : optionWithDeprecatedKeys.fallbackKeys()) {
            Assertions.assertTrue(
                    fallbackKey.isDeprecated(), "deprecated key not flagged as deprecated");
        }
    }

    @Test
    void testDeprecationFlagForFallbackKeys() {
        final ConfigOption<Integer> optionWithFallbackKeys =
                ConfigOptions.key("key")
                        .intType()
                        .defaultValue(0)
                        .withFallbackKeys("fallback1", "fallback2");

        Assertions.assertTrue(optionWithFallbackKeys.hasFallbackKeys());
        for (final FallbackKey fallbackKey : optionWithFallbackKeys.fallbackKeys()) {
            Assertions.assertFalse(fallbackKey.isDeprecated(), "falback key flagged as deprecated");
        }
    }

    @Test
    void testDeprecationFlagForMixedAlternativeKeys() {
        final ConfigOption<Integer> optionWithMixedKeys =
                ConfigOptions.key("key")
                        .intType()
                        .defaultValue(0)
                        .withDeprecatedKeys("deprecated1", "deprecated2")
                        .withFallbackKeys("fallback1", "fallback2");

        final List<String> fallbackKeys = new ArrayList<>(2);
        final List<String> deprecatedKeys = new ArrayList<>(2);
        for (final FallbackKey alternativeKey : optionWithMixedKeys.fallbackKeys()) {
            if (alternativeKey.isDeprecated()) {
                deprecatedKeys.add(alternativeKey.getKey());
            } else {
                fallbackKeys.add(alternativeKey.getKey());
            }
        }

        Assertions.assertEquals(2, fallbackKeys.size());
        Assertions.assertEquals(2, deprecatedKeys.size());

        MatcherAssert.assertThat(fallbackKeys, containsInAnyOrder("fallback1", "fallback2"));
        MatcherAssert.assertThat(deprecatedKeys, containsInAnyOrder("deprecated1", "deprecated2"));
    }

    @Test
    void testDeprecationForDeprecatedKeys() {
        String[] deprecatedKeys = new String[] {"deprecated1", "deprecated2"};
        final Set<String> expectedDeprecatedKeys = new HashSet<>(Arrays.asList(deprecatedKeys));

        final ConfigOption<Integer> optionWithDeprecatedKeys =
                ConfigOptions.key("key")
                        .intType()
                        .defaultValue(0)
                        .withDeprecatedKeys(deprecatedKeys)
                        .withFallbackKeys("fallback1");

        Assertions.assertTrue(optionWithDeprecatedKeys.hasDeprecatedKeys());
        Assertions.assertEquals(
                expectedDeprecatedKeys, Sets.newHashSet(optionWithDeprecatedKeys.deprecatedKeys()));
    }

    @Test
    void testNoDeprecationForFallbackKeysWithoutDeprecated() {
        final ConfigOption<Integer> optionWithFallbackKeys =
                ConfigOptions.key("key").intType().defaultValue(0).withFallbackKeys("fallback1");

        Assertions.assertFalse(optionWithFallbackKeys.hasDeprecatedKeys());
    }
}
