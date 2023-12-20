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
 * limitations under the License
 */

package org.apache.flink.util;

import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/** Tests for {@link CompressedSerializedValue}. */
public class CompressedSerializedValueTest {
    @Test
    void testSimpleValue() throws Exception {

        final String value = "teststring";

        CompressedSerializedValue<String> v = CompressedSerializedValue.fromObject(value);
        CompressedSerializedValue<String> copy = CommonTestUtils.createCopySerializable(v);

        Assertions.assertEquals(value, v.deserializeValue(getClass().getClassLoader()));
        Assertions.assertEquals(value, copy.deserializeValue(getClass().getClassLoader()));

        Assertions.assertEquals(v, copy);
        Assertions.assertEquals(v.hashCode(), copy.hashCode());

        Assertions.assertNotNull(v.toString());
        Assertions.assertNotNull(copy.toString());

        Assertions.assertNotEquals(0, v.getSize());
        Assertions.assertArrayEquals(v.getByteArray(), copy.getByteArray());

        byte[] bytes = v.getByteArray();
        CompressedSerializedValue<String> saved =
                CompressedSerializedValue.fromBytes(Arrays.copyOf(bytes, bytes.length));
        Assertions.assertEquals(v, saved);
        Assertions.assertArrayEquals(v.getByteArray(), saved.getByteArray());
    }

    @Test(expected = NullPointerException.class)
    public void testNullValue() throws Exception {
        CompressedSerializedValue.fromObject(null);
    }

    @Test(expected = NullPointerException.class)
    public void testFromNullBytes() {
        CompressedSerializedValue.fromBytes(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromEmptyBytes() {
        CompressedSerializedValue.fromBytes(new byte[0]);
    }
}
