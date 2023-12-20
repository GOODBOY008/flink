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

package org.apache.flink.configuration;

import org.apache.flink.core.testutils.CommonTestUtils;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.configuration.MemorySize.MemoryUnit.MEGA_BYTES;
import static org.hamcrest.CoreMatchers.is;

/** Tests for the {@link MemorySize} class. */
public class MemorySizeTest {

    @Test
    void testUnitConversion() {
        final MemorySize zero = MemorySize.ZERO;
        Assertions.assertEquals(0, zero.getBytes());
        Assertions.assertEquals(0, zero.getKibiBytes());
        Assertions.assertEquals(0, zero.getMebiBytes());
        Assertions.assertEquals(0, zero.getGibiBytes());
        Assertions.assertEquals(0, zero.getTebiBytes());

        final MemorySize bytes = new MemorySize(955);
        Assertions.assertEquals(955, bytes.getBytes());
        Assertions.assertEquals(0, bytes.getKibiBytes());
        Assertions.assertEquals(0, bytes.getMebiBytes());
        Assertions.assertEquals(0, bytes.getGibiBytes());
        Assertions.assertEquals(0, bytes.getTebiBytes());

        final MemorySize kilos = new MemorySize(18500);
        Assertions.assertEquals(18500, kilos.getBytes());
        Assertions.assertEquals(18, kilos.getKibiBytes());
        Assertions.assertEquals(0, kilos.getMebiBytes());
        Assertions.assertEquals(0, kilos.getGibiBytes());
        Assertions.assertEquals(0, kilos.getTebiBytes());

        final MemorySize megas = new MemorySize(15 * 1024 * 1024);
        Assertions.assertEquals(15_728_640, megas.getBytes());
        Assertions.assertEquals(15_360, megas.getKibiBytes());
        Assertions.assertEquals(15, megas.getMebiBytes());
        Assertions.assertEquals(0, megas.getGibiBytes());
        Assertions.assertEquals(0, megas.getTebiBytes());

        final MemorySize teras = new MemorySize(2L * 1024 * 1024 * 1024 * 1024 + 10);
        Assertions.assertEquals(2199023255562L, teras.getBytes());
        Assertions.assertEquals(2147483648L, teras.getKibiBytes());
        Assertions.assertEquals(2097152, teras.getMebiBytes());
        Assertions.assertEquals(2048, teras.getGibiBytes());
        Assertions.assertEquals(2, teras.getTebiBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalid() {
        new MemorySize(-1);
    }

    @Test
    void testStandardUtils() throws IOException {
        final MemorySize size = new MemorySize(1234567890L);
        final MemorySize cloned = CommonTestUtils.createCopySerializable(size);

        Assertions.assertEquals(size, cloned);
        Assertions.assertEquals(size.hashCode(), cloned.hashCode());
        Assertions.assertEquals(size.toString(), cloned.toString());
    }

    @Test
    void testParseBytes() {
        Assertions.assertEquals(1234, MemorySize.parseBytes("1234"));
        Assertions.assertEquals(1234, MemorySize.parseBytes("1234b"));
        Assertions.assertEquals(1234, MemorySize.parseBytes("1234 b"));
        Assertions.assertEquals(1234, MemorySize.parseBytes("1234bytes"));
        Assertions.assertEquals(1234, MemorySize.parseBytes("1234 bytes"));
    }

    @Test
    void testParseKibiBytes() {
        Assertions.assertEquals(667766, MemorySize.parse("667766k").getKibiBytes());
        Assertions.assertEquals(667766, MemorySize.parse("667766 k").getKibiBytes());
        Assertions.assertEquals(667766, MemorySize.parse("667766kb").getKibiBytes());
        Assertions.assertEquals(667766, MemorySize.parse("667766 kb").getKibiBytes());
        Assertions.assertEquals(667766, MemorySize.parse("667766kibibytes").getKibiBytes());
        Assertions.assertEquals(667766, MemorySize.parse("667766 kibibytes").getKibiBytes());
    }

    @Test
    void testParseMebiBytes() {
        Assertions.assertEquals(7657623, MemorySize.parse("7657623m").getMebiBytes());
        Assertions.assertEquals(7657623, MemorySize.parse("7657623 m").getMebiBytes());
        Assertions.assertEquals(7657623, MemorySize.parse("7657623mb").getMebiBytes());
        Assertions.assertEquals(7657623, MemorySize.parse("7657623 mb").getMebiBytes());
        Assertions.assertEquals(7657623, MemorySize.parse("7657623mebibytes").getMebiBytes());
        Assertions.assertEquals(7657623, MemorySize.parse("7657623 mebibytes").getMebiBytes());
    }

    @Test
    void testParseGibiBytes() {
        Assertions.assertEquals(987654, MemorySize.parse("987654g").getGibiBytes());
        Assertions.assertEquals(987654, MemorySize.parse("987654 g").getGibiBytes());
        Assertions.assertEquals(987654, MemorySize.parse("987654gb").getGibiBytes());
        Assertions.assertEquals(987654, MemorySize.parse("987654 gb").getGibiBytes());
        Assertions.assertEquals(987654, MemorySize.parse("987654gibibytes").getGibiBytes());
        Assertions.assertEquals(987654, MemorySize.parse("987654 gibibytes").getGibiBytes());
    }

    @Test
    void testParseTebiBytes() {
        Assertions.assertEquals(1234567, MemorySize.parse("1234567t").getTebiBytes());
        Assertions.assertEquals(1234567, MemorySize.parse("1234567 t").getTebiBytes());
        Assertions.assertEquals(1234567, MemorySize.parse("1234567tb").getTebiBytes());
        Assertions.assertEquals(1234567, MemorySize.parse("1234567 tb").getTebiBytes());
        Assertions.assertEquals(1234567, MemorySize.parse("1234567tebibytes").getTebiBytes());
        Assertions.assertEquals(1234567, MemorySize.parse("1234567 tebibytes").getTebiBytes());
    }

    @Test
    void testUpperCase() {
        Assertions.assertEquals(1L, MemorySize.parse("1 B").getBytes());
        Assertions.assertEquals(1L, MemorySize.parse("1 K").getKibiBytes());
        Assertions.assertEquals(1L, MemorySize.parse("1 M").getMebiBytes());
        Assertions.assertEquals(1L, MemorySize.parse("1 G").getGibiBytes());
        Assertions.assertEquals(1L, MemorySize.parse("1 T").getTebiBytes());
    }

    @Test
    void testTrimBeforeParse() {
        Assertions.assertEquals(155L, MemorySize.parseBytes("      155      "));
        Assertions.assertEquals(155L, MemorySize.parseBytes("      155      bytes   "));
    }

    @Test
    void testParseInvalid() {
        // null
        try {
            MemorySize.parseBytes(null);
            Assertions.fail("exception expected");
        } catch (NullPointerException ignored) {
        }

        // empty
        try {
            MemorySize.parseBytes("");
            Assertions.fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // blank
        try {
            MemorySize.parseBytes("     ");
            Assertions.fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // no number
        try {
            MemorySize.parseBytes("foobar or fubar or foo bazz");
            Assertions.fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // wrong unit
        try {
            MemorySize.parseBytes("16 gjah");
            Assertions.fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // multiple numbers
        try {
            MemorySize.parseBytes("16 16 17 18 bytes");
            Assertions.fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // negative number
        try {
            MemorySize.parseBytes("-100 bytes");
            Assertions.fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseNumberOverflow() {
        MemorySize.parseBytes("100000000000000000000000000000000 bytes");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseNumberTimeUnitOverflow() {
        MemorySize.parseBytes("100000000000000 tb");
    }

    @Test
    void testParseWithDefaultUnit() {
        Assertions.assertEquals(7, MemorySize.parse("7", MEGA_BYTES).getMebiBytes());
        Assertions.assertNotEquals(7, MemorySize.parse("7340032", MEGA_BYTES));
        Assertions.assertEquals(7, MemorySize.parse("7m", MEGA_BYTES).getMebiBytes());
        Assertions.assertEquals(7168, MemorySize.parse("7", MEGA_BYTES).getKibiBytes());
        Assertions.assertEquals(7168, MemorySize.parse("7m", MEGA_BYTES).getKibiBytes());
        Assertions.assertEquals(7, MemorySize.parse("7 m", MEGA_BYTES).getMebiBytes());
        Assertions.assertEquals(7, MemorySize.parse("7mb", MEGA_BYTES).getMebiBytes());
        Assertions.assertEquals(7, MemorySize.parse("7 mb", MEGA_BYTES).getMebiBytes());
        Assertions.assertEquals(7, MemorySize.parse("7mebibytes", MEGA_BYTES).getMebiBytes());
        Assertions.assertEquals(7, MemorySize.parse("7 mebibytes", MEGA_BYTES).getMebiBytes());
    }

    @Test
    void testDivideByLong() {
        final MemorySize memory = new MemorySize(100L);
        MatcherAssert.assertThat(memory.divide(23), is(new MemorySize(4L)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDivideByNegativeLong() {
        final MemorySize memory = new MemorySize(100L);
        memory.divide(-23L);
    }

    @Test
    void testToHumanReadableString() {
        MatcherAssert.assertThat(new MemorySize(0L).toHumanReadableString(), is("0 bytes"));
        MatcherAssert.assertThat(new MemorySize(1L).toHumanReadableString(), is("1 bytes"));
        MatcherAssert.assertThat(new MemorySize(1024L).toHumanReadableString(), is("1024 bytes"));
        MatcherAssert.assertThat(
                new MemorySize(1025L).toHumanReadableString(), is("1.001kb (1025 bytes)"));
        MatcherAssert.assertThat(
                new MemorySize(1536L).toHumanReadableString(), is("1.500kb (1536 bytes)"));
        MatcherAssert.assertThat(
                new MemorySize(1_000_000L).toHumanReadableString(),
                is("976.563kb (1000000 bytes)"));
        MatcherAssert.assertThat(
                new MemorySize(1_000_000_000L).toHumanReadableString(),
                is("953.674mb (1000000000 bytes)"));
        MatcherAssert.assertThat(
                new MemorySize(1_000_000_000_000L).toHumanReadableString(),
                is("931.323gb (1000000000000 bytes)"));
        MatcherAssert.assertThat(
                new MemorySize(1_000_000_000_000_000L).toHumanReadableString(),
                is("909.495tb (1000000000000000 bytes)"));
    }
}
