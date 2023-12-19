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

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for ResourceSpec class, including its all public api: isValid, lessThanOrEqual, equals,
 * hashCode and merge.
 */
public class ResourceSpecTest extends TestLogger {
    private static final String EXTERNAL_RESOURCE_NAME = "gpu";

    @Test
    void testLessThanOrEqualWhenBothSpecified() {
        ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
        Assertions.assertTrue(rs1.lessThanOrEqual(rs2));
        Assertions.assertTrue(rs2.lessThanOrEqual(rs1));

        ResourceSpec rs3 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        Assertions.assertTrue(rs1.lessThanOrEqual(rs3));
        Assertions.assertFalse(rs3.lessThanOrEqual(rs1));

        ResourceSpec rs4 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        Assertions.assertFalse(rs4.lessThanOrEqual(rs3));
        Assertions.assertTrue(rs3.lessThanOrEqual(rs4));
    }

    @Test
    void testLessThanOrEqualWhenBothUnknown() {
        Assertions.assertTrue(ResourceSpec.UNKNOWN.lessThanOrEqual(ResourceSpec.UNKNOWN));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLessThanOrEqualWhenUnknownWithSpecified() {
        final ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        Assertions.assertTrue(ResourceSpec.UNKNOWN.lessThanOrEqual(rs1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLessThanOrEqualWhenSpecifiedWithUnknown() {
        final ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        Assertions.assertTrue(rs1.lessThanOrEqual(ResourceSpec.UNKNOWN));
    }

    @Test
    void testEquals() {
        ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
        Assertions.assertEquals(rs1, rs2);
        Assertions.assertEquals(rs2, rs1);

        ResourceSpec rs3 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        ResourceSpec rs4 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        Assertions.assertNotEquals(rs3, rs4);

        ResourceSpec rs5 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        Assertions.assertEquals(rs3, rs5);
    }

    @Test
    void testHashCode() {
        ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();
        Assertions.assertEquals(rs1.hashCode(), rs2.hashCode());

        ResourceSpec rs3 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        ResourceSpec rs4 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        Assertions.assertNotEquals(rs3.hashCode(), rs4.hashCode());

        ResourceSpec rs5 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2))
                        .build();
        Assertions.assertEquals(rs3.hashCode(), rs5.hashCode());
    }

    @Test
    void testMerge() {
        ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        ResourceSpec rs2 = ResourceSpec.newBuilder(1.0, 100).build();

        ResourceSpec rs3 = rs1.merge(rs2);
        Assertions.assertEquals(new CPUResource(2.0), rs3.getCpuCores());
        Assertions.assertEquals(200, rs3.getTaskHeapMemory().getMebiBytes());
        Assertions.assertEquals(
                new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1),
                rs3.getExtendedResource(EXTERNAL_RESOURCE_NAME).get());

        ResourceSpec rs4 = rs1.merge(rs3);
        Assertions.assertEquals(
                new ExternalResource(EXTERNAL_RESOURCE_NAME, 2.2),
                rs4.getExtendedResource(EXTERNAL_RESOURCE_NAME).get());
    }

    @Test
    void testSerializable() throws Exception {
        ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();

        ResourceSpec rs2 = CommonTestUtils.createCopySerializable(rs1);
        Assertions.assertEquals(rs1, rs2);
    }

    @Test
    void testMergeThisUnknown() {
        final ResourceSpec spec1 = ResourceSpec.UNKNOWN;
        final ResourceSpec spec2 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();

        final ResourceSpec merged = spec1.merge(spec2);

        Assertions.assertEquals(ResourceSpec.UNKNOWN, merged);
    }

    @Test
    void testMergeOtherUnknown() {
        final ResourceSpec spec1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        final ResourceSpec spec2 = ResourceSpec.UNKNOWN;

        final ResourceSpec merged = spec1.merge(spec2);

        Assertions.assertEquals(ResourceSpec.UNKNOWN, merged);
    }

    @Test
    void testMergeBothUnknown() {
        final ResourceSpec spec1 = ResourceSpec.UNKNOWN;
        final ResourceSpec spec2 = ResourceSpec.UNKNOWN;

        final ResourceSpec merged = spec1.merge(spec2);

        Assertions.assertEquals(ResourceSpec.UNKNOWN, merged);
    }

    @Test
    void testMergeWithSerializationCopy() throws Exception {
        final ResourceSpec spec1 = CommonTestUtils.createCopySerializable(ResourceSpec.UNKNOWN);
        final ResourceSpec spec2 = CommonTestUtils.createCopySerializable(ResourceSpec.UNKNOWN);

        final ResourceSpec merged = spec1.merge(spec2);

        Assertions.assertEquals(ResourceSpec.UNKNOWN, merged);
    }

    @Test
    void testSingletonPropertyOfUnknown() throws Exception {
        final ResourceSpec copiedSpec =
                CommonTestUtils.createCopySerializable(ResourceSpec.UNKNOWN);

        Assertions.assertSame(ResourceSpec.UNKNOWN, copiedSpec);
    }

    @Test
    void testSubtract() {
        final ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        final ResourceSpec rs2 =
                ResourceSpec.newBuilder(0.2, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 0.5))
                        .build();

        final ResourceSpec subtracted = rs1.subtract(rs2);
        Assertions.assertEquals(new CPUResource(0.8), subtracted.getCpuCores());
        Assertions.assertEquals(0, subtracted.getTaskHeapMemory().getMebiBytes());
        Assertions.assertEquals(
                new ExternalResource(EXTERNAL_RESOURCE_NAME, 0.6),
                subtracted.getExtendedResource(EXTERNAL_RESOURCE_NAME).get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubtractOtherHasLargerResources() {
        final ResourceSpec rs1 = ResourceSpec.newBuilder(1.0, 100).build();
        final ResourceSpec rs2 = ResourceSpec.newBuilder(0.2, 200).build();

        rs1.subtract(rs2);
    }

    @Test
    void testSubtractThisUnknown() {
        final ResourceSpec rs1 = ResourceSpec.UNKNOWN;
        final ResourceSpec rs2 =
                ResourceSpec.newBuilder(0.2, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 0.5))
                        .build();

        final ResourceSpec subtracted = rs1.subtract(rs2);
        Assertions.assertEquals(ResourceSpec.UNKNOWN, subtracted);
    }

    @Test
    void testSubtractOtherUnknown() {
        final ResourceSpec rs1 =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.1))
                        .build();
        final ResourceSpec rs2 = ResourceSpec.UNKNOWN;

        final ResourceSpec subtracted = rs1.subtract(rs2);
        Assertions.assertEquals(ResourceSpec.UNKNOWN, subtracted);
    }

    @Test
    void testZeroExtendedResourceFromConstructor() {
        final ResourceSpec resourceSpec =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 0))
                        .build();
        Assertions.assertEquals(resourceSpec.getExtendedResources().size(), 0);
    }

    @Test
    void testZeroExtendedResourceFromSubtract() {
        final ResourceSpec resourceSpec =
                ResourceSpec.newBuilder(1.0, 100)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1.0))
                        .build();

        Assertions.assertEquals(
                resourceSpec.subtract(resourceSpec).getExtendedResources().size(), 0);
    }
}
