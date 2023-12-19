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

class IntMinimumTest {

    @Test
    void testGet() {
        IntMinimum min = new IntMinimum();
        Assertions.assertEquals(Integer.MAX_VALUE, min.getLocalValue().intValue());
    }

    @Test
    void testResetLocal() {
        IntMinimum min = new IntMinimum();
        int value = 13;

        min.add(value);
        Assertions.assertEquals(value, min.getLocalValue().intValue());

        min.resetLocal();
        Assertions.assertEquals(Integer.MAX_VALUE, min.getLocalValue().intValue());
    }

    @Test
    void testAdd() {
        IntMinimum min = new IntMinimum();

        min.add(1234);
        min.add(9876);
        min.add(-987);
        min.add(-123);

        Assertions.assertEquals(-987, min.getLocalValue().intValue());
    }

    @Test
    void testMerge() {
        IntMinimum min1 = new IntMinimum();
        min1.add(1234);

        IntMinimum min2 = new IntMinimum();
        min2.add(5678);

        min2.merge(min1);
        Assertions.assertEquals(1234, min2.getLocalValue().intValue());

        min1.merge(min2);
        Assertions.assertEquals(1234, min1.getLocalValue().intValue());
    }

    @Test
    void testClone() {
        IntMinimum min = new IntMinimum();
        int value = 42;

        min.add(value);

        IntMinimum clone = min.clone();
        Assertions.assertEquals(value, clone.getLocalValue().intValue());
    }
}
