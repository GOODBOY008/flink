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

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.hamcrest.Matchers.is;

/** Tests for the {@link MathUtils}. */
public class MathUtilTest {

    @Test
    void testLog2Computation() {
        Assertions.assertEquals(0, MathUtils.log2floor(1));
        Assertions.assertEquals(1, MathUtils.log2floor(2));
        Assertions.assertEquals(1, MathUtils.log2floor(3));
        Assertions.assertEquals(2, MathUtils.log2floor(4));
        Assertions.assertEquals(2, MathUtils.log2floor(5));
        Assertions.assertEquals(2, MathUtils.log2floor(7));
        Assertions.assertEquals(3, MathUtils.log2floor(8));
        Assertions.assertEquals(3, MathUtils.log2floor(9));
        Assertions.assertEquals(4, MathUtils.log2floor(16));
        Assertions.assertEquals(4, MathUtils.log2floor(17));
        Assertions.assertEquals(13, MathUtils.log2floor((0x1 << 13) + 1));
        Assertions.assertEquals(30, MathUtils.log2floor(Integer.MAX_VALUE));
        Assertions.assertEquals(31, MathUtils.log2floor(-1));

        try {
            MathUtils.log2floor(0);
            Assertions.fail();
        } catch (ArithmeticException ignored) {
        }
    }

    @Test
    void testRoundDownToPowerOf2() {
        Assertions.assertEquals(0, MathUtils.roundDownToPowerOf2(0));
        Assertions.assertEquals(1, MathUtils.roundDownToPowerOf2(1));
        Assertions.assertEquals(2, MathUtils.roundDownToPowerOf2(2));
        Assertions.assertEquals(2, MathUtils.roundDownToPowerOf2(3));
        Assertions.assertEquals(4, MathUtils.roundDownToPowerOf2(4));
        Assertions.assertEquals(4, MathUtils.roundDownToPowerOf2(5));
        Assertions.assertEquals(4, MathUtils.roundDownToPowerOf2(6));
        Assertions.assertEquals(4, MathUtils.roundDownToPowerOf2(7));
        Assertions.assertEquals(8, MathUtils.roundDownToPowerOf2(8));
        Assertions.assertEquals(8, MathUtils.roundDownToPowerOf2(9));
        Assertions.assertEquals(8, MathUtils.roundDownToPowerOf2(15));
        Assertions.assertEquals(16, MathUtils.roundDownToPowerOf2(16));
        Assertions.assertEquals(16, MathUtils.roundDownToPowerOf2(17));
        Assertions.assertEquals(16, MathUtils.roundDownToPowerOf2(31));
        Assertions.assertEquals(32, MathUtils.roundDownToPowerOf2(32));
        Assertions.assertEquals(32, MathUtils.roundDownToPowerOf2(33));
        Assertions.assertEquals(32, MathUtils.roundDownToPowerOf2(42));
        Assertions.assertEquals(32, MathUtils.roundDownToPowerOf2(63));
        Assertions.assertEquals(64, MathUtils.roundDownToPowerOf2(64));
        Assertions.assertEquals(64, MathUtils.roundDownToPowerOf2(125));
        Assertions.assertEquals(16384, MathUtils.roundDownToPowerOf2(25654));
        Assertions.assertEquals(33554432, MathUtils.roundDownToPowerOf2(34366363));
        Assertions.assertEquals(33554432, MathUtils.roundDownToPowerOf2(63463463));
        Assertions.assertEquals(1073741824, MathUtils.roundDownToPowerOf2(1852987883));
        Assertions.assertEquals(1073741824, MathUtils.roundDownToPowerOf2(Integer.MAX_VALUE));
    }

    @Test
    void testRoundUpToPowerOf2() {
        Assertions.assertEquals(0, MathUtils.roundUpToPowerOfTwo(0));
        Assertions.assertEquals(1, MathUtils.roundUpToPowerOfTwo(1));
        Assertions.assertEquals(2, MathUtils.roundUpToPowerOfTwo(2));
        Assertions.assertEquals(4, MathUtils.roundUpToPowerOfTwo(3));
        Assertions.assertEquals(4, MathUtils.roundUpToPowerOfTwo(4));
        Assertions.assertEquals(8, MathUtils.roundUpToPowerOfTwo(5));
        Assertions.assertEquals(8, MathUtils.roundUpToPowerOfTwo(6));
        Assertions.assertEquals(8, MathUtils.roundUpToPowerOfTwo(7));
        Assertions.assertEquals(8, MathUtils.roundUpToPowerOfTwo(8));
        Assertions.assertEquals(16, MathUtils.roundUpToPowerOfTwo(9));
        Assertions.assertEquals(16, MathUtils.roundUpToPowerOfTwo(15));
        Assertions.assertEquals(16, MathUtils.roundUpToPowerOfTwo(16));
        Assertions.assertEquals(32, MathUtils.roundUpToPowerOfTwo(17));
        Assertions.assertEquals(32, MathUtils.roundUpToPowerOfTwo(31));
        Assertions.assertEquals(32, MathUtils.roundUpToPowerOfTwo(32));
        Assertions.assertEquals(64, MathUtils.roundUpToPowerOfTwo(33));
        Assertions.assertEquals(64, MathUtils.roundUpToPowerOfTwo(42));
        Assertions.assertEquals(64, MathUtils.roundUpToPowerOfTwo(63));
        Assertions.assertEquals(64, MathUtils.roundUpToPowerOfTwo(64));
        Assertions.assertEquals(128, MathUtils.roundUpToPowerOfTwo(125));
        Assertions.assertEquals(32768, MathUtils.roundUpToPowerOfTwo(25654));
        Assertions.assertEquals(67108864, MathUtils.roundUpToPowerOfTwo(34366363));
        Assertions.assertEquals(67108864, MathUtils.roundUpToPowerOfTwo(67108863));
        Assertions.assertEquals(67108864, MathUtils.roundUpToPowerOfTwo(67108864));
        Assertions.assertEquals(0x40000000, MathUtils.roundUpToPowerOfTwo(0x3FFFFFFE));
        Assertions.assertEquals(0x40000000, MathUtils.roundUpToPowerOfTwo(0x3FFFFFFF));
        Assertions.assertEquals(0x40000000, MathUtils.roundUpToPowerOfTwo(0x40000000));
    }

    @Test
    void testPowerOfTwo() {
        Assertions.assertTrue(MathUtils.isPowerOf2(1));
        Assertions.assertTrue(MathUtils.isPowerOf2(2));
        Assertions.assertTrue(MathUtils.isPowerOf2(4));
        Assertions.assertTrue(MathUtils.isPowerOf2(8));
        Assertions.assertTrue(MathUtils.isPowerOf2(32768));
        Assertions.assertTrue(MathUtils.isPowerOf2(65536));
        Assertions.assertTrue(MathUtils.isPowerOf2(1 << 30));
        Assertions.assertTrue(MathUtils.isPowerOf2(1L + Integer.MAX_VALUE));
        Assertions.assertTrue(MathUtils.isPowerOf2(1L << 41));
        Assertions.assertTrue(MathUtils.isPowerOf2(1L << 62));

        Assertions.assertFalse(MathUtils.isPowerOf2(3));
        Assertions.assertFalse(MathUtils.isPowerOf2(5));
        Assertions.assertFalse(MathUtils.isPowerOf2(567923));
        Assertions.assertFalse(MathUtils.isPowerOf2(Integer.MAX_VALUE));
        Assertions.assertFalse(MathUtils.isPowerOf2(Long.MAX_VALUE));
    }

    @Test
    void testFlipSignBit() {
        Assertions.assertEquals(0L, MathUtils.flipSignBit(Long.MIN_VALUE));
        Assertions.assertEquals(Long.MIN_VALUE, MathUtils.flipSignBit(0L));
        Assertions.assertEquals(-1L, MathUtils.flipSignBit(Long.MAX_VALUE));
        Assertions.assertEquals(Long.MAX_VALUE, MathUtils.flipSignBit(-1L));
        Assertions.assertEquals(42L | Long.MIN_VALUE, MathUtils.flipSignBit(42L));
        Assertions.assertEquals(-42L & Long.MAX_VALUE, MathUtils.flipSignBit(-42L));
    }

    @Test
    void testDivideRoundUp() {
        MatcherAssert.assertThat(MathUtils.divideRoundUp(0, 1), is(0));
        MatcherAssert.assertThat(MathUtils.divideRoundUp(0, 2), is(0));
        MatcherAssert.assertThat(MathUtils.divideRoundUp(1, 1), is(1));
        MatcherAssert.assertThat(MathUtils.divideRoundUp(1, 2), is(1));
        MatcherAssert.assertThat(MathUtils.divideRoundUp(2, 1), is(2));
        MatcherAssert.assertThat(MathUtils.divideRoundUp(2, 2), is(1));
        MatcherAssert.assertThat(MathUtils.divideRoundUp(2, 3), is(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDivideRoundUpNegativeDividend() {
        MathUtils.divideRoundUp(-1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDivideRoundUpNegativeDivisor() {
        MathUtils.divideRoundUp(1, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDivideRoundUpZeroDivisor() {
        MathUtils.divideRoundUp(1, 0);
    }
}
