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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.SerializerTestInstance;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class EnumSerializerTest extends TestLogger {

    @Test
    void testPublicEnum() {
        testEnumSerializer(PrivateEnum.ONE, PrivateEnum.TWO, PrivateEnum.THREE);
    }

    @Test
    void testPrivateEnum() {
        testEnumSerializer(
                PublicEnum.FOO,
                PublicEnum.BAR,
                PublicEnum.PETER,
                PublicEnum.NATHANIEL,
                PublicEnum.EMMA,
                PublicEnum.PAULA);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyEnum() {
        new EnumSerializer<>(EmptyEnum.class);
    }

    @Test
    void testReconfiguration() {
        // mock the previous ordering of enum constants to be BAR, PAULA, NATHANIEL
        PublicEnum[] mockPreviousOrder = {PublicEnum.BAR, PublicEnum.PAULA, PublicEnum.NATHANIEL};

        // now, the actual order of FOO, BAR, PETER, NATHANIEL, EMMA, PAULA will be the "new wrong
        // order"
        EnumSerializer<PublicEnum> serializer = new EnumSerializer<>(PublicEnum.class);

        // verify that the serializer is first using the "wrong order" (i.e., the initial new
        // configuration)
        Assertions.assertEquals(
                PublicEnum.FOO.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue());
        Assertions.assertEquals(
                PublicEnum.BAR.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue());
        Assertions.assertEquals(
                PublicEnum.PETER.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue());
        Assertions.assertEquals(
                PublicEnum.NATHANIEL.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue());
        Assertions.assertEquals(
                PublicEnum.EMMA.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue());
        Assertions.assertEquals(
                PublicEnum.PAULA.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue());

        // reconfigure and verify compatibility
        EnumSerializer.EnumSerializerSnapshot serializerSnapshot =
                new EnumSerializer.EnumSerializerSnapshot(PublicEnum.class, mockPreviousOrder);
        TypeSerializerSchemaCompatibility compatibility =
                serializerSnapshot.resolveSchemaCompatibility(serializer);
        Assertions.assertTrue(compatibility.isCompatibleWithReconfiguredSerializer());

        // after reconfiguration, the order should be first the original BAR, PAULA, NATHANIEL,
        // followed by the "new enum constants" FOO, PETER, EMMA
        PublicEnum[] expectedOrder = {
            PublicEnum.BAR,
            PublicEnum.PAULA,
            PublicEnum.NATHANIEL,
            PublicEnum.FOO,
            PublicEnum.PETER,
            PublicEnum.EMMA
        };

        EnumSerializer<PublicEnum> configuredSerializer =
                (EnumSerializer<PublicEnum>) compatibility.getReconfiguredSerializer();
        int i = 0;
        for (PublicEnum constant : expectedOrder) {
            Assertions.assertEquals(
                    i, configuredSerializer.getValueToOrdinal().get(constant).intValue());
            i++;
        }

        Assertions.assertArrayEquals(expectedOrder, configuredSerializer.getValues());
    }

    @Test
    void testConfigurationSnapshotSerialization() throws Exception {
        EnumSerializer<PublicEnum> serializer = new EnumSerializer<>(PublicEnum.class);

        byte[] serializedConfig;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                    new DataOutputViewStreamWrapper(out), serializer.snapshotConfiguration());
            serializedConfig = out.toByteArray();
        }

        TypeSerializerSnapshot<PublicEnum> restoredConfig;
        try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
            restoredConfig =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        TypeSerializerSchemaCompatibility<PublicEnum> compatResult =
                restoredConfig.resolveSchemaCompatibility(serializer);
        Assertions.assertTrue(compatResult.isCompatibleAsIs());

        Assertions.assertEquals(
                PublicEnum.FOO.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue());
        Assertions.assertEquals(
                PublicEnum.BAR.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue());
        Assertions.assertEquals(
                PublicEnum.PETER.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue());
        Assertions.assertEquals(
                PublicEnum.NATHANIEL.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue());
        Assertions.assertEquals(
                PublicEnum.EMMA.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue());
        Assertions.assertEquals(
                PublicEnum.PAULA.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue());
        Assertions.assertArrayEquals(PublicEnum.values(), serializer.getValues());
    }

    @Test
    void testSerializeEnumSerializer() throws Exception {
        EnumSerializer<PublicEnum> serializer = new EnumSerializer<>(PublicEnum.class);

        // verify original transient parameters
        Assertions.assertEquals(
                PublicEnum.FOO.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue());
        Assertions.assertEquals(
                PublicEnum.BAR.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue());
        Assertions.assertEquals(
                PublicEnum.PETER.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue());
        Assertions.assertEquals(
                PublicEnum.NATHANIEL.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue());
        Assertions.assertEquals(
                PublicEnum.EMMA.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue());
        Assertions.assertEquals(
                PublicEnum.PAULA.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue());
        Assertions.assertArrayEquals(PublicEnum.values(), serializer.getValues());

        byte[] serializedSerializer = InstantiationUtil.serializeObject(serializer);

        // deserialize and re-verify transient parameters
        serializer =
                InstantiationUtil.deserializeObject(
                        serializedSerializer, Thread.currentThread().getContextClassLoader());
        Assertions.assertEquals(
                PublicEnum.FOO.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue());
        Assertions.assertEquals(
                PublicEnum.BAR.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue());
        Assertions.assertEquals(
                PublicEnum.PETER.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue());
        Assertions.assertEquals(
                PublicEnum.NATHANIEL.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue());
        Assertions.assertEquals(
                PublicEnum.EMMA.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue());
        Assertions.assertEquals(
                PublicEnum.PAULA.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue());
        Assertions.assertArrayEquals(PublicEnum.values(), serializer.getValues());
    }

    @Test
    void testSerializeReconfiguredEnumSerializer() {
        // mock the previous ordering of enum constants to be BAR, PAULA, NATHANIEL
        PublicEnum[] mockPreviousOrder = {PublicEnum.BAR, PublicEnum.PAULA, PublicEnum.NATHANIEL};

        // now, the actual order of FOO, BAR, PETER, NATHANIEL, EMMA, PAULA will be the "new wrong
        // order"
        EnumSerializer<PublicEnum> serializer = new EnumSerializer<>(PublicEnum.class);

        // verify that the serializer is first using the "wrong order" (i.e., the initial new
        // configuration)
        Assertions.assertEquals(
                PublicEnum.FOO.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.FOO).intValue());
        Assertions.assertEquals(
                PublicEnum.BAR.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.BAR).intValue());
        Assertions.assertEquals(
                PublicEnum.PETER.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.PETER).intValue());
        Assertions.assertEquals(
                PublicEnum.NATHANIEL.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.NATHANIEL).intValue());
        Assertions.assertEquals(
                PublicEnum.EMMA.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.EMMA).intValue());
        Assertions.assertEquals(
                PublicEnum.PAULA.ordinal(),
                serializer.getValueToOrdinal().get(PublicEnum.PAULA).intValue());

        // reconfigure and verify compatibility
        EnumSerializer.EnumSerializerSnapshot serializerSnapshot =
                new EnumSerializer.EnumSerializerSnapshot(PublicEnum.class, mockPreviousOrder);
        TypeSerializerSchemaCompatibility compatibility =
                serializerSnapshot.resolveSchemaCompatibility(serializer);
        Assertions.assertTrue(compatibility.isCompatibleWithReconfiguredSerializer());

        // verify that after the serializer was read, the reconfigured constant ordering is
        // untouched
        PublicEnum[] expectedOrder = {
            PublicEnum.BAR,
            PublicEnum.PAULA,
            PublicEnum.NATHANIEL,
            PublicEnum.FOO,
            PublicEnum.PETER,
            PublicEnum.EMMA
        };

        EnumSerializer<PublicEnum> configuredSerializer =
                (EnumSerializer<PublicEnum>) compatibility.getReconfiguredSerializer();
        int i = 0;
        for (PublicEnum constant : expectedOrder) {
            Assertions.assertEquals(
                    i, configuredSerializer.getValueToOrdinal().get(constant).intValue());
            i++;
        }

        Assertions.assertArrayEquals(expectedOrder, configuredSerializer.getValues());
    }

    @SafeVarargs
    public final <T extends Enum<T>> void testEnumSerializer(T... data) {
        @SuppressWarnings("unchecked")
        final Class<T> clazz = (Class<T>) data.getClass().getComponentType();

        SerializerTestInstance<T> tester =
                new SerializerTestInstance<T>(new EnumSerializer<>(clazz), clazz, 4, data) {};

        tester.testAll();
    }

    // ------------------------------------------------------------------------
    //  Test enums
    // ------------------------------------------------------------------------

    public enum PublicEnum {
        FOO,
        BAR,
        PETER,
        NATHANIEL,
        EMMA,
        PAULA
    }

    public enum EmptyEnum {}

    private enum PrivateEnum {
        ONE,
        TWO,
        THREE
    }
}
