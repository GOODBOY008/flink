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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.flink.util.TernaryBoolean.FALSE;
import static org.apache.flink.util.TernaryBoolean.TRUE;
import static org.apache.flink.util.TernaryBoolean.UNDEFINED;

/** Tests for the {@link TernaryBoolean} class. */
class TernaryBooleanTest {

    @Test
    void testWithDefault() {
        Assertions.assertTrue(TRUE.getOrDefault(true));
        Assertions.assertTrue(TRUE.getOrDefault(false));

        Assertions.assertFalse(FALSE.getOrDefault(true));
        Assertions.assertFalse(FALSE.getOrDefault(false));

        Assertions.assertTrue(UNDEFINED.getOrDefault(true));
        Assertions.assertFalse(UNDEFINED.getOrDefault(false));
    }

    @Test
    void testResolveUndefined() {
        Assertions.assertEquals(TRUE, TRUE.resolveUndefined(true));
        Assertions.assertEquals(TRUE, TRUE.resolveUndefined(false));

        Assertions.assertEquals(FALSE, FALSE.resolveUndefined(true));
        Assertions.assertEquals(FALSE, FALSE.resolveUndefined(false));

        Assertions.assertEquals(TRUE, UNDEFINED.resolveUndefined(true));
        Assertions.assertEquals(FALSE, UNDEFINED.resolveUndefined(false));
    }

    @Test
    void testToBoolean() {
        Assertions.assertSame(Boolean.TRUE, TRUE.getAsBoolean());
        Assertions.assertSame(Boolean.FALSE, FALSE.getAsBoolean());
        Assertions.assertNull(UNDEFINED.getAsBoolean());
    }

    @Test
    void testFromBoolean() {
        Assertions.assertEquals(TRUE, TernaryBoolean.fromBoolean(true));
        Assertions.assertEquals(FALSE, TernaryBoolean.fromBoolean(false));
    }

    @Test
    void testFromBoxedBoolean() {
        Assertions.assertEquals(TRUE, TernaryBoolean.fromBoxedBoolean(Boolean.TRUE));
        Assertions.assertEquals(FALSE, TernaryBoolean.fromBoxedBoolean(Boolean.FALSE));
        Assertions.assertEquals(UNDEFINED, TernaryBoolean.fromBoxedBoolean(null));
    }
}
