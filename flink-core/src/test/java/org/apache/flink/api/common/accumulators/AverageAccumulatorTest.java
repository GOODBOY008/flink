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

package org.apache.flink.api.common.accumulators;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AverageAccumulatorTest {

    @Test
    public void testGet() {
        AverageAccumulator average = new AverageAccumulator();
        Assertions.assertEquals(0.0, average.getLocalValue(), 0.0);
    }

    @Test
    public void testAdd() {
        AverageAccumulator average = new AverageAccumulator();
        int i1;
        for (i1 = 0; i1 < 10; i1++) {
            average.add(i1);
        }
        Assertions.assertEquals(4.5, average.getLocalValue(), 0.0);
        average.resetLocal();

        Integer i2;
        for (i2 = 0; i2 < 10; i2++) {
            average.add(i2);
        }
        Assertions.assertEquals(4.5, average.getLocalValue(), 0.0);
        average.resetLocal();

        long i3;
        for (i3 = 0; i3 < 10; i3++) {
            average.add(i3);
        }
        Assertions.assertEquals(4.5, average.getLocalValue(), 0.0);
        average.resetLocal();

        Long i4;
        for (i4 = 0L; i4 < 10; i4++) {
            average.add(i4);
        }
        Assertions.assertEquals(4.5, average.getLocalValue(), 0.0);
        average.resetLocal();

        double i5;
        for (i5 = 0; i5 < 10; i5++) {
            average.add(i5);
        }
        Assertions.assertEquals(4.5, average.getLocalValue(), 0.0);
        average.resetLocal();

        Double i6;
        for (i6 = 0.0; i6 < 10; i6++) {
            average.add(i6);
        }
        Assertions.assertEquals(4.5, average.getLocalValue(), 0.0);
        average.resetLocal();

        Assertions.assertEquals(0.0, average.getLocalValue(), 0.0);
    }

    @Test
    public void testMergeSuccess() {
        AverageAccumulator avg1 = new AverageAccumulator();
        for (int i = 0; i < 5; i++) {
            avg1.add(i);
        }

        AverageAccumulator avg2 = new AverageAccumulator();
        for (int i = 5; i < 10; i++) {
            avg2.add(i);
        }

        avg1.merge(avg2);
        Assertions.assertEquals(4.5, avg1.getLocalValue(), 0.0);
    }

    @Test
    public void testMergeFailed() {
        AverageAccumulator average = new AverageAccumulator();
        Accumulator<Double, Double> averageNew = null;
        average.add(1);
        try {
            average.merge(averageNew);
            Assertions.fail("should fail with an exception");
        } catch (IllegalArgumentException e) {
            Assertions.assertNotNull(e.getMessage());
            Assertions.assertTrue(
                    e.getMessage().contains("The merged accumulator must be AverageAccumulator."));
        } catch (Throwable t) {
            Assertions.fail(
                    "wrong exception; expected IllegalArgumentException but found "
                            + t.getClass().getName());
        }
    }

    @Test
    public void testClone() {
        AverageAccumulator average = new AverageAccumulator();
        average.add(1);
        AverageAccumulator averageNew = average.clone();
        Assertions.assertEquals(1, averageNew.getLocalValue(), 0.0);
    }
}
