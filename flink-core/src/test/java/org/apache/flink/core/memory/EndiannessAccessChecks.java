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

package org.apache.flink.core.memory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

/**
 * Verifies correct accesses with regards to endianness in {@link MemorySegment} (in both heap and
 * off-heap modes).
 */
class EndiannessAccessChecks {

    @Test
    void testOnHeapSegment() {
        testBigAndLittleEndianAccessUnaligned(MemorySegmentFactory.wrap(new byte[11111]));
    }

    @Test
    void testOffHeapSegment() {
        testBigAndLittleEndianAccessUnaligned(
                MemorySegmentFactory.allocateUnpooledOffHeapMemory(11111));
    }

    @Test
    void testOffHeapUnsafeSegment() {
        testBigAndLittleEndianAccessUnaligned(
                MemorySegmentFactory.allocateOffHeapUnsafeMemory(11111));
    }

    private void testBigAndLittleEndianAccessUnaligned(MemorySegment segment) {
        final Random rnd = new Random();

        // longs
        {
            final long seed = rnd.nextLong();

            rnd.setSeed(seed);
            for (int i = 0; i < 10000; i++) {
                long val = rnd.nextLong();
                int pos = rnd.nextInt(segment.size() - 7);

                segment.putLongLittleEndian(pos, val);
                long r = segment.getLongBigEndian(pos);
                Assertions.assertEquals(val, Long.reverseBytes(r));

                segment.putLongBigEndian(pos, val);
                r = segment.getLongLittleEndian(pos);
                Assertions.assertEquals(val, Long.reverseBytes(r));
            }
        }

        // ints
        {
            final long seed = rnd.nextLong();

            rnd.setSeed(seed);
            for (int i = 0; i < 10000; i++) {
                int val = rnd.nextInt();
                int pos = rnd.nextInt(segment.size() - 3);

                segment.putIntLittleEndian(pos, val);
                int r = segment.getIntBigEndian(pos);
                Assertions.assertEquals(val, Integer.reverseBytes(r));

                segment.putIntBigEndian(pos, val);
                r = segment.getIntLittleEndian(pos);
                Assertions.assertEquals(val, Integer.reverseBytes(r));
            }
        }

        // shorts
        {
            final long seed = rnd.nextLong();

            rnd.setSeed(seed);
            for (int i = 0; i < 10000; i++) {
                short val = (short) rnd.nextInt();
                int pos = rnd.nextInt(segment.size() - 1);

                segment.putShortLittleEndian(pos, val);
                short r = segment.getShortBigEndian(pos);
                Assertions.assertEquals(val, Short.reverseBytes(r));

                segment.putShortBigEndian(pos, val);
                r = segment.getShortLittleEndian(pos);
                Assertions.assertEquals(val, Short.reverseBytes(r));
            }
        }

        // chars
        {
            final long seed = rnd.nextLong();

            rnd.setSeed(seed);
            for (int i = 0; i < 10000; i++) {
                char val = (char) rnd.nextInt();
                int pos = rnd.nextInt(segment.size() - 1);

                segment.putCharLittleEndian(pos, val);
                char r = segment.getCharBigEndian(pos);
                Assertions.assertEquals(val, Character.reverseBytes(r));

                segment.putCharBigEndian(pos, val);
                r = segment.getCharLittleEndian(pos);
                Assertions.assertEquals(val, Character.reverseBytes(r));
            }
        }

        // floats
        {
            final long seed = rnd.nextLong();

            rnd.setSeed(seed);
            for (int i = 0; i < 10000; i++) {
                float val = rnd.nextFloat();
                int pos = rnd.nextInt(segment.size() - 3);

                segment.putFloatLittleEndian(pos, val);
                float r = segment.getFloatBigEndian(pos);
                float reversed =
                        Float.intBitsToFloat(Integer.reverseBytes(Float.floatToRawIntBits(r)));
                Assertions.assertEquals(val, reversed, 0.0f);

                segment.putFloatBigEndian(pos, val);
                r = segment.getFloatLittleEndian(pos);
                reversed = Float.intBitsToFloat(Integer.reverseBytes(Float.floatToRawIntBits(r)));
                Assertions.assertEquals(val, reversed, 0.0f);
            }
        }

        // doubles
        {
            final long seed = rnd.nextLong();

            rnd.setSeed(seed);
            for (int i = 0; i < 10000; i++) {
                double val = rnd.nextDouble();
                int pos = rnd.nextInt(segment.size() - 7);

                segment.putDoubleLittleEndian(pos, val);
                double r = segment.getDoubleBigEndian(pos);
                double reversed =
                        Double.longBitsToDouble(Long.reverseBytes(Double.doubleToRawLongBits(r)));
                Assertions.assertEquals(val, reversed, 0.0f);

                segment.putDoubleBigEndian(pos, val);
                r = segment.getDoubleLittleEndian(pos);
                reversed =
                        Double.longBitsToDouble(Long.reverseBytes(Double.doubleToRawLongBits(r)));
                Assertions.assertEquals(val, reversed, 0.0f);
            }
        }
    }
}
