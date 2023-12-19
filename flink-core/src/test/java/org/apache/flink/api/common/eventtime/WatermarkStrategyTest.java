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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;

/** Test for the {@link WatermarkStrategy} class. */
class WatermarkStrategyTest {

    @Test
    void testDefaultTimeStampAssigner() {
        WatermarkStrategy<Object> wmStrategy = WatermarkStrategy.forMonotonousTimestamps();

        // ensure that the closure can be cleaned through the Watermark Strategies
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        MatcherAssert.assertThat(
                wmStrategy.createTimestampAssigner(assignerContext()),
                instanceOf(RecordTimestampAssigner.class));
    }

    @Test
    void testLambdaTimestampAssigner() {
        WatermarkStrategy<Object> wmStrategy =
                WatermarkStrategy.forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> 42L);

        // ensure that the closure can be cleaned through the Watermark Strategies
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        TimestampAssigner<Object> timestampAssigner =
                wmStrategy.createTimestampAssigner(assignerContext());

        MatcherAssert.assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
    }

    @Test
    void testLambdaTimestampAssignerSupplier() {
        WatermarkStrategy<Object> wmStrategy =
                WatermarkStrategy.forMonotonousTimestamps()
                        .withTimestampAssigner(
                                TimestampAssignerSupplier.of((event, timestamp) -> 42L));
        // ensure that the closure can be cleaned through the Watermark Strategies
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        TimestampAssigner<Object> timestampAssigner =
                wmStrategy.createTimestampAssigner(assignerContext());

        MatcherAssert.assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
    }

    @Test
    void testAnonymousInnerTimestampAssigner() {
        WatermarkStrategy<Object> wmStrategy =
                WatermarkStrategy.forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Object>)
                                        (element, recordTimestamp) -> 42);
        // ensure that the closure can be cleaned through the Watermark Strategies
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        TimestampAssigner<Object> timestampAssigner =
                wmStrategy.createTimestampAssigner(assignerContext());

        MatcherAssert.assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
    }

    @Test
    void testClassTimestampAssigner() {
        WatermarkStrategy<Object> wmStrategy =
                WatermarkStrategy.forMonotonousTimestamps()
                        .withTimestampAssigner((ctx) -> new TestTimestampAssigner());
        // ensure that the closure can be cleaned through the Watermark Strategies
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        TimestampAssigner<Object> timestampAssigner =
                wmStrategy.createTimestampAssigner(assignerContext());

        MatcherAssert.assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
    }

    @Test
    void testClassTimestampAssignerUsingSupplier() {
        WatermarkStrategy<Object> wmStrategy =
                WatermarkStrategy.forMonotonousTimestamps()
                        .withTimestampAssigner((context) -> new TestTimestampAssigner());
        // ensure that the closure can be cleaned through the Watermark Strategies
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        TimestampAssigner<Object> timestampAssigner =
                wmStrategy.createTimestampAssigner(assignerContext());

        MatcherAssert.assertThat(timestampAssigner.extractTimestamp(null, 13L), is(42L));
    }

    @Test
    void testWithIdlenessHelper() {
        WatermarkStrategy<String> wmStrategy =
                WatermarkStrategy.<String>forMonotonousTimestamps()
                        .withIdleness(Duration.ofDays(7));

        // ensure that the closure can be cleaned
        ClosureCleaner.clean(wmStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        MatcherAssert.assertThat(
                wmStrategy.createTimestampAssigner(assignerContext()),
                instanceOf(RecordTimestampAssigner.class));
        MatcherAssert.assertThat(
                wmStrategy.createWatermarkGenerator(generatorContext()),
                instanceOf(WatermarksWithIdleness.class));
    }

    @Test
    void testWithWatermarkAlignment() {
        final String watermarkGroup = "group-1";
        final Duration maxAllowedWatermarkDrift = Duration.ofMillis(200);
        final WatermarkStrategy<String> strategy =
                WatermarkStrategy.<String>forMonotonousTimestamps()
                        .withWatermarkAlignment(watermarkGroup, maxAllowedWatermarkDrift)
                        // we call a different builder method on top of watermark alignment
                        // to make sure it can be properly mixed
                        .withIdleness(Duration.ofMillis(200));

        // ensure that the closure can be cleaned
        ClosureCleaner.clean(strategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        final WatermarkAlignmentParams alignmentParameters = strategy.getAlignmentParameters();
        MatcherAssert.assertThat(alignmentParameters.getWatermarkGroup(), equalTo(watermarkGroup));
        MatcherAssert.assertThat(
                alignmentParameters.getMaxAllowedWatermarkDrift(),
                equalTo(maxAllowedWatermarkDrift.toMillis()));
        MatcherAssert.assertThat(
                alignmentParameters.getUpdateInterval(),
                equalTo(WatermarksWithWatermarkAlignment.DEFAULT_UPDATE_INTERVAL.toMillis()));
        MatcherAssert.assertThat(
                strategy.createTimestampAssigner(assignerContext()),
                instanceOf(RecordTimestampAssigner.class));
        MatcherAssert.assertThat(
                strategy.createWatermarkGenerator(generatorContext()),
                instanceOf(WatermarksWithIdleness.class));
    }

    static class TestTimestampAssigner implements TimestampAssigner<Object>, Serializable {

        @Override
        public long extractTimestamp(Object element, long recordTimestamp) {
            return 42L;
        }
    }

    static TimestampAssignerSupplier.Context assignerContext() {
        return DummyMetricGroup::new;
    }

    static WatermarkGeneratorSupplier.Context generatorContext() {
        return DummyMetricGroup::new;
    }

    /**
     * A dummy {@link MetricGroup} to be used when a group is required as an argument but not
     * actually used.
     */
    public static class DummyMetricGroup implements MetricGroup {

        @Override
        public Counter counter(String name) {
            return null;
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            return null;
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            return null;
        }

        @Override
        public <H extends Histogram> H histogram(String name, H histogram) {
            return null;
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            return null;
        }

        @Override
        public MetricGroup addGroup(String name) {
            return null;
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            return null;
        }

        @Override
        public String[] getScopeComponents() {
            return new String[0];
        }

        @Override
        public Map<String, String> getAllVariables() {
            return null;
        }

        @Override
        public String getMetricIdentifier(String metricName) {
            return null;
        }

        @Override
        public String getMetricIdentifier(String metricName, CharacterFilter filter) {
            return null;
        }
    }
}
