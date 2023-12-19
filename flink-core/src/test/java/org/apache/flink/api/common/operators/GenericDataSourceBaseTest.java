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

package org.apache.flink.api.common.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.util.TestIOData;
import org.apache.flink.api.common.operators.util.TestNonRichInputFormat;
import org.apache.flink.api.common.operators.util.TestRichInputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;

/** Checks the GenericDataSourceBase operator for both Rich and non-Rich input formats. */
@SuppressWarnings("serial")
public class GenericDataSourceBaseTest implements java.io.Serializable {

    @Test
    public void testDataSourcePlain() {
        try {
            TestNonRichInputFormat in = new TestNonRichInputFormat();
            GenericDataSourceBase<String, TestNonRichInputFormat> source =
                    new GenericDataSourceBase<String, TestNonRichInputFormat>(
                            in,
                            new OperatorInformation<String>(BasicTypeInfo.STRING_TYPE_INFO),
                            "testSource");

            ExecutionConfig executionConfig = new ExecutionConfig();
            executionConfig.disableObjectReuse();
            List<String> resultMutableSafe = source.executeOnCollections(null, executionConfig);

            in.reset();
            executionConfig.enableObjectReuse();
            List<String> resultRegular = source.executeOnCollections(null, executionConfig);
            Assertions.assertEquals(asList(TestIOData.NAMES), resultMutableSafe);
            Assertions.assertEquals(asList(TestIOData.NAMES), resultRegular);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testDataSourceWithRuntimeContext() {
        try {
            TestRichInputFormat in = new TestRichInputFormat();
            GenericDataSourceBase<String, TestRichInputFormat> source =
                    new GenericDataSourceBase<String, TestRichInputFormat>(
                            in,
                            new OperatorInformation<String>(BasicTypeInfo.STRING_TYPE_INFO),
                            "testSource");

            final HashMap<String, Accumulator<?, ?>> accumulatorMap =
                    new HashMap<String, Accumulator<?, ?>>();
            final HashMap<String, Future<Path>> cpTasks = new HashMap<>();
            final TaskInfo taskInfo = new TaskInfo("test_source", 1, 0, 1, 0);

            ExecutionConfig executionConfig = new ExecutionConfig();
            executionConfig.disableObjectReuse();
            Assertions.assertEquals(false, in.hasBeenClosed());
            Assertions.assertEquals(false, in.hasBeenOpened());

            List<String> resultMutableSafe =
                    source.executeOnCollections(
                            new RuntimeUDFContext(
                                    taskInfo,
                                    null,
                                    executionConfig,
                                    cpTasks,
                                    accumulatorMap,
                                    UnregisteredMetricsGroup.createOperatorMetricGroup()),
                            executionConfig);

            Assertions.assertEquals(true, in.hasBeenClosed());
            Assertions.assertEquals(true, in.hasBeenOpened());

            in.reset();
            executionConfig.enableObjectReuse();
            Assertions.assertEquals(false, in.hasBeenClosed());
            Assertions.assertEquals(false, in.hasBeenOpened());

            List<String> resultRegular =
                    source.executeOnCollections(
                            new RuntimeUDFContext(
                                    taskInfo,
                                    null,
                                    executionConfig,
                                    cpTasks,
                                    accumulatorMap,
                                    UnregisteredMetricsGroup.createOperatorMetricGroup()),
                            executionConfig);

            Assertions.assertEquals(true, in.hasBeenClosed());
            Assertions.assertEquals(true, in.hasBeenOpened());

            Assertions.assertEquals(asList(TestIOData.RICH_NAMES), resultMutableSafe);
            Assertions.assertEquals(asList(TestIOData.RICH_NAMES), resultRegular);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail(e.getMessage());
        }
    }
}
