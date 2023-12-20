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

import org.assertj.core.api.Assertions;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.AssertionsForClassTypes.within;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Tests for the {@link DelegatingConfiguration}. */
public class DelegatingConfigurationTest {

    @Test
    void testIfDelegatesImplementAllMethods() throws IllegalArgumentException {

        // For each method in the Configuration class...
        Method[] confMethods = Configuration.class.getDeclaredMethods();
        Method[] delegateMethods = DelegatingConfiguration.class.getDeclaredMethods();

        for (Method configurationMethod : confMethods) {
            final int mod = configurationMethod.getModifiers();
            if (!Modifier.isPublic(mod) || Modifier.isStatic(mod)) {
                continue;
            }

            boolean hasMethod = false;

            // Find matching method in wrapper class and call it
            lookForWrapper:
            for (Method wrapperMethod : delegateMethods) {
                if (configurationMethod.getName().equals(wrapperMethod.getName())) {

                    // Get parameters for method
                    Class<?>[] wrapperMethodParams = wrapperMethod.getParameterTypes();
                    Class<?>[] configMethodParams = configurationMethod.getParameterTypes();
                    if (wrapperMethodParams.length != configMethodParams.length) {
                        continue;
                    }

                    for (int i = 0; i < wrapperMethodParams.length; i++) {
                        if (wrapperMethodParams[i] != configMethodParams[i]) {
                            continue lookForWrapper;
                        }
                    }
                    hasMethod = true;
                    break;
                }
            }

            org.junit.jupiter.api.Assertions.assertTrue(
                    hasMethod,
                    "Configuration method '"
                            + configurationMethod.getName()
                            + "' has not been wrapped correctly in DelegatingConfiguration wrapper");
        }
    }

    @Test
    void testDelegationConfigurationWithNullPrefix() {
        Configuration backingConf = new Configuration();
        backingConf.setValueInternal("test-key", "value", false);

        DelegatingConfiguration configuration = new DelegatingConfiguration(backingConf, null);
        Set<String> keySet = configuration.keySet();

        org.junit.jupiter.api.Assertions.assertThat(backingConf.keySet()).isEqualTo(keySet);
    }

    @Test
    void testDelegationConfigurationWithPrefix() {
        String prefix = "pref-";
        String expectedKey = "key";

        /*
         * Key matches the prefix
         */
        Configuration backingConf = new Configuration();
        backingConf.setValueInternal(prefix + expectedKey, "value", false);

        DelegatingConfiguration configuration = new DelegatingConfiguration(backingConf, prefix);
        Set<String> keySet = configuration.keySet();

        org.junit.jupiter.api.Assertions.assertThat(1).isEqualTo(keySet.size());
        org.junit.jupiter.api.Assertions.assertThat(expectedKey).isEqualTo(keySet.iterator().next());

        /*
         * Key does not match the prefix
         */
        backingConf = new Configuration();
        backingConf.setValueInternal("test-key", "value", false);

        configuration = new DelegatingConfiguration(backingConf, prefix);
        keySet = configuration.keySet();

        org.junit.jupiter.api.Assertions.assertThat(keySet.isEmpty()).isTrue();
    }

    @Test
    void testDelegationConfigurationToMapConsistentWithAddAllToProperties() {
        Configuration conf = new Configuration();
        conf.setString("k0", "v0");
        conf.setString("prefix.k1", "v1");
        conf.setString("prefix.prefix.k2", "v2");
        conf.setString("k3.prefix.prefix.k3", "v3");
        DelegatingConfiguration dc = new DelegatingConfiguration(conf, "prefix.");
        // Collect all properties
        Properties properties = new Properties();
        dc.addAllToProperties(properties);
        // Convert the Map<String, String> object into a Properties object
        Map<String, String> map = dc.toMap();
        Properties mapProperties = new Properties();
      mapProperties.putAll(map);
        // Verification
        org.junit.jupiter.api.Assertions.assertThat(mapProperties).isEqualTo(properties);
    }

    @Test
    void testSetReturnsDelegatingConfiguration() {
        final Configuration conf = new Configuration();
        final DelegatingConfiguration delegatingConf = new DelegatingConfiguration(conf, "prefix.");

   assertThat(delegatingConf.set(CoreOptions.DEFAULT_PARALLELISM, 1))
                .isSameAs(delegatingConf);
    }
}
