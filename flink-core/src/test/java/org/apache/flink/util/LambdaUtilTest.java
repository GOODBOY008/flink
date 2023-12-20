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

package org.apache.flink.util;


import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.AssertionsForClassTypes.within;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.net.URLClassLoader;

/** Tests for the {@link LambdaUtil}. */
public class LambdaUtilTest {

    @Test
    void testRunWithContextClassLoaderRunnable() {
        final ClassLoader aPrioriContextClassLoader =
                Thread.currentThread().getContextClassLoader();

        try {
            final ClassLoader original = new URLClassLoader(new URL[0]);
            final ClassLoader temp = new URLClassLoader(new URL[0]);

            // set the original context class loader
            Thread.currentThread().setContextClassLoader(original);

            LambdaUtil.withContextClassLoader(
                    temp,
                    () ->
                       assertSame(
                                    temp, Thread.currentThread().getContextClassLoader()));

            // make sure the method restored the original context class loader
       assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(original);
        } finally {
            Thread.currentThread().setContextClassLoader(aPrioriContextClassLoader);
        }
    }

    @Test
    void testRunWithContextClassLoaderSupplier() {
        final ClassLoader aPrioriContextClassLoader =
                Thread.currentThread().getContextClassLoader();

        try {
            final ClassLoader original = new URLClassLoader(new URL[0]);
            final ClassLoader temp = new URLClassLoader(new URL[0]);

            // set the original context class loader
            Thread.currentThread().setContextClassLoader(original);

            LambdaUtil.withContextClassLoader(
                    temp,
                    () -> {
                   assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(temp);
                        return true;
                    });

            // make sure the method restored the original context class loader
       assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(original);
        } finally {
            Thread.currentThread().setContextClassLoader(aPrioriContextClassLoader);
        }
    }
}
