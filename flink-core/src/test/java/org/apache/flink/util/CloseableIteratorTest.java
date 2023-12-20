/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

/** {@link CloseableIterator} test. */
@SuppressWarnings("unchecked")
public class CloseableIteratorTest {

    private static final String[] ELEMENTS = new String[] {"element-1", "element-2"};

    @Test
    void testFlattenEmpty() throws Exception {
        List<CloseableIterator<?>> iterators =
                asList(
                        CloseableIterator.flatten(),
                        CloseableIterator.flatten(CloseableIterator.empty()),
                        CloseableIterator.flatten(CloseableIterator.flatten()));
        for (CloseableIterator<?> i : iterators) {
            Assertions.assertFalse(i.hasNext());
            i.close();
        }
    }

    @Test
    void testFlattenIteration() {
        CloseableIterator<String> iterator =
                CloseableIterator.flatten(
                        CloseableIterator.ofElement(ELEMENTS[0], unused -> {}),
                        CloseableIterator.ofElement(ELEMENTS[1], unused -> {}));

        List<String> iterated = new ArrayList<>();
        iterator.forEachRemaining(iterated::add);
        Assertions.assertArrayEquals(ELEMENTS, iterated.toArray());
    }

    @Test(expected = TestException.class)
     void testFlattenErrorHandling() throws Exception {
        List<String> closed = new ArrayList<>();
        CloseableIterator<String> iterator =
                CloseableIterator.flatten(
                        CloseableIterator.ofElement(
                                ELEMENTS[0],
                                e -> {
                                    closed.add(e);
                                    throw new TestException();
                                }),
                        CloseableIterator.ofElement(ELEMENTS[1], closed::add));
        try {
            iterator.close();
        } finally {
            Assertions.assertArrayEquals(ELEMENTS, closed.toArray());
        }
    }

    private static class TestException extends RuntimeException {}
}
