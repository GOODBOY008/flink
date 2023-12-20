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

package org.apache.flink.util.concurrent;

import org.apache.flink.util.TestLogger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.AssertionsForClassTypes.within;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IncrementalDelayRetryStrategy}. */
public class IncrementalDelayRetryStrategyTest extends TestLogger {

    @Test
    void testGettersNotCapped() {
        RetryStrategy retryStrategy =
                new IncrementalDelayRetryStrategy(
                        10, Duration.ofMillis(5L), Duration.ofMillis(4L), Duration.ofMillis(20L));
        assertThat(retryStrategy.getNumRemainingRetries()).isEqualTo(10);
        assertThat(retryStrategy.getRetryDelay()).isEqualTo(Duration.ofMillis(5L));

        RetryStrategy nextRetryStrategy = retryStrategy.getNextRetryStrategy();
        assertThat(nextRetryStrategy.getNumRemainingRetries()).isEqualTo(9);
        assertThat(nextRetryStrategy.getRetryDelay()).isEqualTo(Duration.ofMillis(9L));
    }

    @Test
    void testGettersHitCapped() {
        RetryStrategy retryStrategy =
                new IncrementalDelayRetryStrategy(
                        5, Duration.ofMillis(15L), Duration.ofMillis(10L), Duration.ofMillis(20L));
        assertThat(retryStrategy.getNumRemainingRetries()).isEqualTo(5);
        assertThat(retryStrategy.getRetryDelay()).isEqualTo(Duration.ofMillis(15L));

        RetryStrategy nextRetryStrategy = retryStrategy.getNextRetryStrategy();
        assertThat(nextRetryStrategy.getNumRemainingRetries()).isEqualTo(4);
        assertThat(nextRetryStrategy.getRetryDelay()).isEqualTo(Duration.ofMillis(20L));
    }

    @Test
    void testGettersAtCap() {
        RetryStrategy retryStrategy =
                new IncrementalDelayRetryStrategy(
                        5, Duration.ofMillis(20L), Duration.ofMillis(5L), Duration.ofMillis(20L));
        assertThat(retryStrategy.getNumRemainingRetries()).isEqualTo(5);
        assertThat(retryStrategy.getRetryDelay()).isEqualTo(Duration.ofMillis(20L));

        RetryStrategy nextRetryStrategy = retryStrategy.getNextRetryStrategy();
        assertThat(nextRetryStrategy.getNumRemainingRetries()).isEqualTo(4);
        assertThat(nextRetryStrategy.getRetryDelay()).isEqualTo(Duration.ofMillis(20L));
    }

    /** Tests that getting a next RetryStrategy below zero remaining retries fails. */
    @Test
    void testRetryFailure() {
        assertThatThrownBy(
                        () ->
                                new IncrementalDelayRetryStrategy(
                                                0,
                                                Duration.ofMillis(20L),
                                                Duration.ofMillis(5L),
                                                Duration.ofMillis(20L))
                                        .getNextRetryStrategy())
                .isInstanceOf(IllegalStateException.class);
    }
}
