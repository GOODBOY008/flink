/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.CollectionUtil.HASH_MAP_DEFAULT_LOAD_FACTOR;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for java collection utilities. */
@ExtendWith(TestLoggerExtension.class)
public class CollectionUtilTest {

    @Test
    void testPartition() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        Collection<List<Integer>> partitioned = CollectionUtil.partition(list, 4);

        assertThat(partitioned)
                .as("List partitioned into the an incorrect number of partitions")
                .hasSize(4);
        assertThat(partitioned).allSatisfy(partition -> assertThat(partition).hasSize(1));
    }

    @Test
    void testOfNullableWithNull() {
        assertThat(CollectionUtil.ofNullable(null)).isEmpty();
    }

    @Test
    void testFromNullableWithObject() {
        final Object element = new Object();
        assertThat(CollectionUtil.ofNullable(element)).singleElement().isEqualTo(element);
    }

    @Test
    void testComputeCapacity() {
   assertEquals(
                1, CollectionUtil.computeRequiredCapacity(0, HASH_MAP_DEFAULT_LOAD_FACTOR));
   assertEquals(
                2, CollectionUtil.computeRequiredCapacity(1, HASH_MAP_DEFAULT_LOAD_FACTOR));
   assertEquals(
                3, CollectionUtil.computeRequiredCapacity(2, HASH_MAP_DEFAULT_LOAD_FACTOR));
   assertEquals(
                4, CollectionUtil.computeRequiredCapacity(3, HASH_MAP_DEFAULT_LOAD_FACTOR));
   assertEquals(
                6, CollectionUtil.computeRequiredCapacity(4, HASH_MAP_DEFAULT_LOAD_FACTOR));
   assertEquals(
                7, CollectionUtil.computeRequiredCapacity(5, HASH_MAP_DEFAULT_LOAD_FACTOR));
   assertEquals(
                8, CollectionUtil.computeRequiredCapacity(6, HASH_MAP_DEFAULT_LOAD_FACTOR));
   assertEquals(
                10, CollectionUtil.computeRequiredCapacity(7, HASH_MAP_DEFAULT_LOAD_FACTOR));
   assertEquals(
                11, CollectionUtil.computeRequiredCapacity(8, HASH_MAP_DEFAULT_LOAD_FACTOR));
   assertEquals(
                134, CollectionUtil.computeRequiredCapacity(100, HASH_MAP_DEFAULT_LOAD_FACTOR));
   assertEquals(
                1334, CollectionUtil.computeRequiredCapacity(1000, HASH_MAP_DEFAULT_LOAD_FACTOR));
   assertEquals(
                13334, CollectionUtil.computeRequiredCapacity(10000, HASH_MAP_DEFAULT_LOAD_FACTOR));

   assertThat(CollectionUtil.computeRequiredCapacity(10000).isCloseTo(20000, within(0.5f)));

   assertThat(CollectionUtil.computeRequiredCapacity(10000).isCloseTo(100000, within(0.1f)));

   assertEquals(
                1431655808,
                CollectionUtil.computeRequiredCapacity(
                        Integer.MAX_VALUE / 2, HASH_MAP_DEFAULT_LOAD_FACTOR));
   assertEquals(
                Integer.MAX_VALUE,
                CollectionUtil.computeRequiredCapacity(
                        1 + Integer.MAX_VALUE / 2, HASH_MAP_DEFAULT_LOAD_FACTOR));

        try {
            CollectionUtil.computeRequiredCapacity(-1, HASH_MAP_DEFAULT_LOAD_FACTOR);
       fail();
        } catch (IllegalArgumentException expected) {
        }

        try {
            CollectionUtil.computeRequiredCapacity(Integer.MIN_VALUE, HASH_MAP_DEFAULT_LOAD_FACTOR);
       fail();
        } catch (IllegalArgumentException expected) {
        }
    }
}
