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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.api.common.eventtime.WatermarksWithIdleness.IdlenessTimer;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/** Test for the {@link WatermarksWithIdleness} class. */
class WatermarksWithIdlenessTest {

    @Test(expected = IllegalArgumentException.class)
    void testZeroTimeout() {
        new WatermarksWithIdleness<>(new AscendingTimestampsWatermarks<>(), Duration.ZERO);
    }

    @Test(expected = IllegalArgumentException.class)
    void testNegativeTimeout() {
        new WatermarksWithIdleness<>(new AscendingTimestampsWatermarks<>(), Duration.ofMillis(-1L));
    }

    @Test
    void testInitiallyActive() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = new IdlenessTimer(clock, Duration.ofMillis(10));

        Assertions.assertFalse(timer.checkIfIdle());
    }

    @Test
    void testIdleWithoutEvents() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = new IdlenessTimer(clock, Duration.ofMillis(10));
        timer.checkIfIdle(); // start timer

        clock.advanceTime(11, MILLISECONDS);
        Assertions.assertTrue(timer.checkIfIdle());
    }

    @Test
    void testRepeatedIdleChecks() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = createTimerAndMakeIdle(clock, Duration.ofMillis(122));

        Assertions.assertTrue(timer.checkIfIdle());
        clock.advanceTime(100, MILLISECONDS);
        Assertions.assertTrue(timer.checkIfIdle());
    }

    @Test
    void testActiveAfterIdleness() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = createTimerAndMakeIdle(clock, Duration.ofMillis(10));

        timer.activity();
        Assertions.assertFalse(timer.checkIfIdle());
    }

    @Test
    void testIdleActiveIdle() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = createTimerAndMakeIdle(clock, Duration.ofMillis(122));

        // active again
        timer.activity();
        Assertions.assertFalse(timer.checkIfIdle());

        // idle again
        timer.checkIfIdle(); // start timer
        clock.advanceTime(Duration.ofMillis(123));
        Assertions.assertTrue(timer.checkIfIdle());
    }

    private static IdlenessTimer createTimerAndMakeIdle(ManualClock clock, Duration idleTimeout) {
        final IdlenessTimer timer = new IdlenessTimer(clock, idleTimeout);

        timer.checkIfIdle(); // start timer
        clock.advanceTime(Duration.ofMillis(idleTimeout.toMillis() + 1));
        Assertions.assertTrue(timer.checkIfIdle()); // rigger timer

        return timer;
    }
}
