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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.AssertionsForClassTypes.within;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import static org.apache.flink.configuration.MemorySize.MemoryUnit.MEGA_BYTES;
import static org.hamcrest.CoreMatchers.is;

/** Tests for the {@link MemorySize} class. */
public class MemorySizeTest {

    @Test
    void testUnitConversion() {
        final MemorySize zero = MemorySize.ZERO;
   assertThat(zero.getBytes()).isEqualTo(0);
   assertThat(zero.getKibiBytes()).isEqualTo(0);
   assertThat(zero.getMebiBytes()).isEqualTo(0);
   assertThat(zero.getGibiBytes()).isEqualTo(0);
   assertThat(zero.getTebiBytes()).isEqualTo(0);

        final MemorySize bytes = new MemorySize(955);
   assertThat(bytes.getBytes()).isEqualTo(955);
   assertThat(bytes.getKibiBytes()).isEqualTo(0);
   assertThat(bytes.getMebiBytes()).isEqualTo(0);
   assertThat(bytes.getGibiBytes()).isEqualTo(0);
   assertThat(bytes.getTebiBytes()).isEqualTo(0);

        final MemorySize kilos = new MemorySize(18500);
   assertThat(kilos.getBytes()).isEqualTo(18500);
   assertThat(kilos.getKibiBytes()).isEqualTo(18);
   assertThat(kilos.getMebiBytes()).isEqualTo(0);
   assertThat(kilos.getGibiBytes()).isEqualTo(0);
   assertThat(kilos.getTebiBytes()).isEqualTo(0);

        final MemorySize megas = new MemorySize(15 * 1024 * 1024);
   assertThat(megas.getBytes()).isEqualTo(15_728_640);
   assertThat(megas.getKibiBytes()).isEqualTo(15_360);
   assertThat(megas.getMebiBytes()).isEqualTo(15);
   assertThat(megas.getGibiBytes()).isEqualTo(0);
   assertThat(megas.getTebiBytes()).isEqualTo(0);

        final MemorySize teras = new MemorySize(2L * 1024 * 1024 * 1024 * 1024 + 10);
   assertThat(teras.getBytes()).isEqualTo(2199023255562L);
   assertThat(teras.getKibiBytes()).isEqualTo(2147483648L);
   assertThat(teras.getMebiBytes()).isEqualTo(2097152);
   assertThat(teras.getGibiBytes()).isEqualTo(2048);
   assertThat(teras.getTebiBytes()).isEqualTo(2);
    }

    @Test(expected = IllegalArgumentException.class)
     void testInvalid() {
        new MemorySize(-1);
    }

    @Test
    void testStandardUtils() throws IOException {
        final MemorySize size = new MemorySize(1234567890L);
        final MemorySize cloned = CommonTestUtils.createCopySerializable(size);

   assertThat(cloned).isEqualTo(size);
   assertThat(cloned.hashCode()).isEqualTo(size.hashCode());
   assertThat(cloned.toString()).isEqualTo(size.toString());
    }

    @Test
    void testParseBytes() {
   assertThat(MemorySize.parseBytes("1234")).isEqualTo(1234);
   assertThat(MemorySize.parseBytes("1234b")).isEqualTo(1234);
   assertThat(MemorySize.parseBytes("1234 b")).isEqualTo(1234);
   assertThat(MemorySize.parseBytes("1234bytes")).isEqualTo(1234);
   assertThat(MemorySize.parseBytes("1234 bytes")).isEqualTo(1234);
    }

    @Test
    void testParseKibiBytes() {
   assertThat(MemorySize.parse("667766k").getKibiBytes()).isEqualTo(667766);
   assertThat(MemorySize.parse("667766 k").getKibiBytes()).isEqualTo(667766);
   assertThat(MemorySize.parse("667766kb").getKibiBytes()).isEqualTo(667766);
   assertThat(MemorySize.parse("667766 kb").getKibiBytes()).isEqualTo(667766);
   assertThat(MemorySize.parse("667766kibibytes").getKibiBytes()).isEqualTo(667766);
   assertThat(MemorySize.parse("667766 kibibytes").getKibiBytes()).isEqualTo(667766);
    }

    @Test
    void testParseMebiBytes() {
   assertThat(MemorySize.parse("7657623m").getMebiBytes()).isEqualTo(7657623);
   assertThat(MemorySize.parse("7657623 m").getMebiBytes()).isEqualTo(7657623);
   assertThat(MemorySize.parse("7657623mb").getMebiBytes()).isEqualTo(7657623);
   assertThat(MemorySize.parse("7657623 mb").getMebiBytes()).isEqualTo(7657623);
   assertThat(MemorySize.parse("7657623mebibytes").getMebiBytes()).isEqualTo(7657623);
   assertThat(MemorySize.parse("7657623 mebibytes").getMebiBytes()).isEqualTo(7657623);
    }

    @Test
    void testParseGibiBytes() {
   assertThat(MemorySize.parse("987654g").getGibiBytes()).isEqualTo(987654);
   assertThat(MemorySize.parse("987654 g").getGibiBytes()).isEqualTo(987654);
   assertThat(MemorySize.parse("987654gb").getGibiBytes()).isEqualTo(987654);
   assertThat(MemorySize.parse("987654 gb").getGibiBytes()).isEqualTo(987654);
   assertThat(MemorySize.parse("987654gibibytes").getGibiBytes()).isEqualTo(987654);
   assertThat(MemorySize.parse("987654 gibibytes").getGibiBytes()).isEqualTo(987654);
    }

    @Test
    void testParseTebiBytes() {
   assertThat(MemorySize.parse("1234567t").getTebiBytes()).isEqualTo(1234567);
   assertThat(MemorySize.parse("1234567 t").getTebiBytes()).isEqualTo(1234567);
   assertThat(MemorySize.parse("1234567tb").getTebiBytes()).isEqualTo(1234567);
   assertThat(MemorySize.parse("1234567 tb").getTebiBytes()).isEqualTo(1234567);
   assertThat(MemorySize.parse("1234567tebibytes").getTebiBytes()).isEqualTo(1234567);
   assertThat(MemorySize.parse("1234567 tebibytes").getTebiBytes()).isEqualTo(1234567);
    }

    @Test
    void testUpperCase() {
   assertThat(MemorySize.parse("1 B").getBytes()).isEqualTo(1L);
   assertThat(MemorySize.parse("1 K").getKibiBytes()).isEqualTo(1L);
   assertThat(MemorySize.parse("1 M").getMebiBytes()).isEqualTo(1L);
   assertThat(MemorySize.parse("1 G").getGibiBytes()).isEqualTo(1L);
   assertThat(MemorySize.parse("1 T").getTebiBytes()).isEqualTo(1L);
    }

    @Test
    void testTrimBeforeParse() {
   assertThat(MemorySize.parseBytes("      155      ")).isEqualTo(155L);
   assertThat(MemorySize.parseBytes("      155      bytes   ")).isEqualTo(155L);
    }

    @Test
    void testParseInvalid() {
        // null
        try {
            MemorySize.parseBytes(null);
       fail("exception expected");
        } catch (NullPointerException ignored) {
        }

        // empty
        try {
            MemorySize.parseBytes("");
       fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // blank
        try {
            MemorySize.parseBytes("     ");
       fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // no number
        try {
            MemorySize.parseBytes("foobar or fubar or foo bazz");
       fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // wrong unit
        try {
            MemorySize.parseBytes("16 gjah");
       fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // multiple numbers
        try {
            MemorySize.parseBytes("16 16 17 18 bytes");
       fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // negative number
        try {
            MemorySize.parseBytes("-100 bytes");
       fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test(expected = IllegalArgumentException.class)
     void testParseNumberOverflow() {
        MemorySize.parseBytes("100000000000000000000000000000000 bytes");
    }

    @Test(expected = IllegalArgumentException.class)
     void testParseNumberTimeUnitOverflow() {
        MemorySize.parseBytes("100000000000000 tb");
    }

    @Test
    void testParseWithDefaultUnit() {
   assertThat(MemorySize.parse("7", MEGA_BYTES).getMebiBytes()).isEqualTo(7);
   assertThat(MemorySize.parse("7340032", MEGA_BYTES)).isNotEqualTo(7);
   assertThat(MemorySize.parse("7m", MEGA_BYTES).getMebiBytes()).isEqualTo(7);
   assertThat(MemorySize.parse("7", MEGA_BYTES).getKibiBytes()).isEqualTo(7168);
   assertThat(MemorySize.parse("7m", MEGA_BYTES).getKibiBytes()).isEqualTo(7168);
   assertThat(MemorySize.parse("7 m", MEGA_BYTES).getMebiBytes()).isEqualTo(7);
   assertThat(MemorySize.parse("7mb", MEGA_BYTES).getMebiBytes()).isEqualTo(7);
   assertThat(MemorySize.parse("7 mb", MEGA_BYTES).getMebiBytes()).isEqualTo(7);
   assertThat(MemorySize.parse("7mebibytes", MEGA_BYTES).getMebiBytes()).isEqualTo(7);
   assertThat(MemorySize.parse("7 mebibytes", MEGA_BYTES).getMebiBytes()).isEqualTo(7);
    }

    @Test
    void testDivideByLong() {
        final MemorySize memory = new MemorySize(100L);
        MatcherAssert.assertThat(memory.divide(23), is(new MemorySize(4L)));
    }

    @Test(expected = IllegalArgumentException.class)
     void testDivideByNegativeLong() {
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
