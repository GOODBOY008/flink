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

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Tests for the {@link ListStateDescriptor}. */
class ListStateDescriptorTest {

    @Test
    void testListStateDescriptor() throws Exception {

        TypeSerializer<String> serializer =
                new KryoSerializer<>(String.class, new ExecutionConfig());

        ListStateDescriptor<String> descr = new ListStateDescriptor<>("testName", serializer);

        Assertions.assertEquals("testName", descr.getName());
        Assertions.assertNotNull(descr.getSerializer());
        Assertions.assertInstanceOf(ListSerializer.class, descr.getSerializer());
        Assertions.assertNotNull(descr.getElementSerializer());
        Assertions.assertEquals(serializer, descr.getElementSerializer());

        ListStateDescriptor<String> copy = CommonTestUtils.createCopySerializable(descr);

        Assertions.assertEquals("testName", copy.getName());
        Assertions.assertNotNull(copy.getSerializer());
        Assertions.assertInstanceOf(ListSerializer.class, copy.getSerializer());

        Assertions.assertNotNull(copy.getElementSerializer());
        Assertions.assertEquals(serializer, copy.getElementSerializer());
    }

    @Test
    void testHashCodeEquals() throws Exception {
        final String name = "testName";

        ListStateDescriptor<String> original = new ListStateDescriptor<>(name, String.class);
        ListStateDescriptor<String> same = new ListStateDescriptor<>(name, String.class);
        ListStateDescriptor<String> sameBySerializer =
                new ListStateDescriptor<>(name, StringSerializer.INSTANCE);

        // test that hashCode() works on state descriptors with initialized and uninitialized
        // serializers
        Assertions.assertEquals(original.hashCode(), same.hashCode());
        Assertions.assertEquals(original.hashCode(), sameBySerializer.hashCode());

        Assertions.assertEquals(original, same);
        Assertions.assertEquals(original, sameBySerializer);

        // equality with a clone
        ListStateDescriptor<String> clone = CommonTestUtils.createCopySerializable(original);
        Assertions.assertEquals(original, clone);

        // equality with an initialized
        clone.initializeSerializerUnlessSet(new ExecutionConfig());
        Assertions.assertEquals(original, clone);

        original.initializeSerializerUnlessSet(new ExecutionConfig());
        Assertions.assertEquals(original, same);
    }

    /**
     * FLINK-6775.
     *
     * <p>Tests that the returned serializer is duplicated. This allows to share the state
     * descriptor.
     */
    @Test
    void testSerializerDuplication() {
        // we need a serializer that actually duplicates for testing (a stateful one)
        // we use Kryo here, because it meets these conditions
        TypeSerializer<String> statefulSerializer =
                new KryoSerializer<>(String.class, new ExecutionConfig());

        ListStateDescriptor<String> descr = new ListStateDescriptor<>("foobar", statefulSerializer);

        TypeSerializer<String> serializerA = descr.getElementSerializer();
        TypeSerializer<String> serializerB = descr.getElementSerializer();

        // check that the retrieved serializers are not the same
        Assertions.assertNotSame(serializerA, serializerB);

        TypeSerializer<List<String>> listSerializerA = descr.getSerializer();
        TypeSerializer<List<String>> listSerializerB = descr.getSerializer();

        Assertions.assertNotSame(listSerializerA, listSerializerB);
    }
}
