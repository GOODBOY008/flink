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

package org.apache.flink.api.common.resources;

import org.apache.flink.util.TestLogger;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

/** Tests for {@link Resource}. */
public class ResourceTest extends TestLogger {

    @Test
    void testConstructorValid() {
        final Resource<TestResource> v1 = new TestResource(0.1);
        assertTestResourceValueEquals(0.1, v1);

        final Resource<TestResource> v2 = new TestResource(BigDecimal.valueOf(0.1));
        assertTestResourceValueEquals(0.1, v2);
    }

    @Test(expected = IllegalArgumentException.class)
    void testConstructorInvalidValue() {
        new TestResource(-0.1);
    }

    @Test
    void testEquals() {
        final Resource<TestResource> v1 = new TestResource(0.1);
        final Resource<TestResource> v2 = new TestResource(0.1);
        final Resource<TestResource> v3 = new TestResource(0.2);
        Assertions.assertEquals(v1, v2);
        Assertions.assertNotEquals(v1, v3);
    }

    @Test
    void testEqualsIgnoringScale() {
        final Resource<TestResource> v1 = new TestResource(new BigDecimal("0.1"));
        final Resource<TestResource> v2 = new TestResource(new BigDecimal("0.10"));
        Assertions.assertEquals(v1, v2);
    }

    @Test
    void testHashCodeIgnoringScale() {
        final Resource<TestResource> v1 = new TestResource(new BigDecimal("0.1"));
        final Resource<TestResource> v2 = new TestResource(new BigDecimal("0.10"));
        Assertions.assertEquals(v1.hashCode(), v2.hashCode());
    }

    @Test
    void testMerge() {
        final Resource<TestResource> v1 = new TestResource(0.1);
        final TestResource v2 = new TestResource(0.2);
        assertTestResourceValueEquals(0.3, v1.merge(v2));
    }

    @Test(expected = IllegalArgumentException.class)
    void testMergeErrorOnDifferentTypes() {
        final Resource<TestResource> v1 = new TestResource(0.1);
        final TestResource v2 = new CPUResource(0.1);
        v1.merge(v2);
    }

    @Test
    void testSubtract() {
        final Resource<TestResource> v1 = new TestResource(0.2);
        final TestResource v2 = new TestResource(0.1);
        assertTestResourceValueEquals(0.1, v1.subtract(v2));
    }

    @Test(expected = IllegalArgumentException.class)
    void testSubtractLargerValue() {
        final Resource<TestResource> v1 = new TestResource(0.1);
        final TestResource v2 = new TestResource(0.2);
        v1.subtract(v2);
    }

    @Test(expected = IllegalArgumentException.class)
    void testSubtractErrorOnDifferentTypes() {
        final Resource<TestResource> v1 = new TestResource(0.1);
        final TestResource v2 = new CPUResource(0.1);
        v1.subtract(v2);
    }

    @Test
    void testDivide() {
        final Resource<TestResource> resource = new TestResource(0.04);
        final BigDecimal by = BigDecimal.valueOf(0.1);
        assertTestResourceValueEquals(0.4, resource.divide(by));
    }

    @Test(expected = IllegalArgumentException.class)
    void testDivideNegative() {
        final Resource<TestResource> resource = new TestResource(1.2);
        final BigDecimal by = BigDecimal.valueOf(-0.5);
        resource.divide(by);
    }

    @Test
    void testDivideInteger() {
        final Resource<TestResource> resource = new TestResource(0.12);
        final int by = 4;
        assertTestResourceValueEquals(0.03, resource.divide(by));
    }

    @Test(expected = IllegalArgumentException.class)
    void testDivideNegativeInteger() {
        final Resource<TestResource> resource = new TestResource(1.2);
        final int by = -5;
        resource.divide(by);
    }

    @Test
    void testMultiply() {
        final Resource<TestResource> resource = new TestResource(0.3);
        final BigDecimal by = BigDecimal.valueOf(0.2);
        assertTestResourceValueEquals(0.06, resource.multiply(by));
    }

    @Test(expected = IllegalArgumentException.class)
    void testMutiplyNegative() {
        final Resource<TestResource> resource = new TestResource(0.3);
        final BigDecimal by = BigDecimal.valueOf(-0.2);
        resource.multiply(by);
    }

    @Test
    void testMultiplyInteger() {
        final Resource<TestResource> resource = new TestResource(0.3);
        final int by = 2;
        assertTestResourceValueEquals(0.6, resource.multiply(by));
    }

    @Test(expected = IllegalArgumentException.class)
    void testMutiplyNegativeInteger() {
        final Resource<TestResource> resource = new TestResource(0.3);
        final int by = -2;
        resource.multiply(by);
    }

    @Test
    void testIsZero() {
        final Resource<TestResource> resource1 = new TestResource(0.0);
        final Resource<TestResource> resource2 = new TestResource(1.0);

        Assertions.assertTrue(resource1.isZero());
        Assertions.assertFalse(resource2.isZero());
    }

    @Test
    void testCompareTo() {
        final Resource<TestResource> resource1 = new TestResource(0.0);
        final Resource<TestResource> resource2 = new TestResource(0.0);
        final Resource<TestResource> resource3 = new TestResource(1.0);

        MatcherAssert.assertThat(resource1.compareTo(resource1), is(0));
        MatcherAssert.assertThat(resource1.compareTo(resource2), is(0));
        MatcherAssert.assertThat(resource1.compareTo(resource3), lessThan(0));
        MatcherAssert.assertThat(resource3.compareTo(resource1), greaterThan(0));
    }

    @Test(expected = IllegalArgumentException.class)
    void testCompareToFailNull() {
        new TestResource(0.0).compareTo(null);
    }

    @Test(expected = IllegalArgumentException.class)
    void testCompareToFailDifferentType() {
        // initialized as different anonymous classes
        final Resource<TestResource> resource1 = new TestResource(0.0) {};
        final Resource<TestResource> resource2 = new TestResource(0.0) {};
        resource1.compareTo(resource2);
    }

    @Test(expected = IllegalArgumentException.class)
    void testCompareToFailDifferentName() {
        // initialized as different anonymous classes
        final Resource<TestResource> resource1 = new TestResource("name1", 0.0);
        final Resource<TestResource> resource2 = new TestResource("name2", 0.0);
        resource1.compareTo(resource2);
    }

    /** This test assume that the scale limitation is 8. */
    @Test
    void testValueScaleLimited() {
        final Resource<TestResource> v1 = new TestResource(0.100000001);
        assertTestResourceValueEquals(0.1, v1);

        final Resource<TestResource> v2 = new TestResource(1.0).divide(3);
        assertTestResourceValueEquals(0.33333333, v2);
    }

    @Test
    void testStripTrailingZeros() {
        final Resource<TestResource> v = new TestResource(0.25).multiply(2);
        MatcherAssert.assertThat(v.getValue().toString(), is("0.5"));
    }

    private static void assertTestResourceValueEquals(final double value, final Resource resource) {
        Assertions.assertEquals(new TestResource(value), resource);
    }
}
