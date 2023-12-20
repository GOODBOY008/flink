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

import org.apache.flink.api.common.time.Time;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

/** Tests for {@link TimeUtils}. */
public class TimeUtilsTest {

    @Test
    void testParseDurationNanos() {
        Assertions.assertEquals(424562, TimeUtils.parseDuration("424562ns").getNano());
        Assertions.assertEquals(424562, TimeUtils.parseDuration("424562nano").getNano());
        Assertions.assertEquals(424562, TimeUtils.parseDuration("424562nanos").getNano());
        Assertions.assertEquals(424562, TimeUtils.parseDuration("424562nanosecond").getNano());
        Assertions.assertEquals(424562, TimeUtils.parseDuration("424562nanoseconds").getNano());
        Assertions.assertEquals(424562, TimeUtils.parseDuration("424562 ns").getNano());
    }

    @Test
    void testParseDurationMicros() {
        Assertions.assertEquals(565731 * 1000L, TimeUtils.parseDuration("565731µs").getNano());
        Assertions.assertEquals(565731 * 1000L, TimeUtils.parseDuration("565731micro").getNano());
        Assertions.assertEquals(565731 * 1000L, TimeUtils.parseDuration("565731micros").getNano());
        Assertions.assertEquals(
                565731 * 1000L, TimeUtils.parseDuration("565731microsecond").getNano());
        Assertions.assertEquals(
                565731 * 1000L, TimeUtils.parseDuration("565731microseconds").getNano());
        Assertions.assertEquals(565731 * 1000L, TimeUtils.parseDuration("565731 µs").getNano());
    }

    @Test
    void testParseDurationMillis() {
        Assertions.assertEquals(1234, TimeUtils.parseDuration("1234").toMillis());
        Assertions.assertEquals(1234, TimeUtils.parseDuration("1234ms").toMillis());
        Assertions.assertEquals(1234, TimeUtils.parseDuration("1234milli").toMillis());
        Assertions.assertEquals(1234, TimeUtils.parseDuration("1234millis").toMillis());
        Assertions.assertEquals(1234, TimeUtils.parseDuration("1234millisecond").toMillis());
        Assertions.assertEquals(1234, TimeUtils.parseDuration("1234milliseconds").toMillis());
        Assertions.assertEquals(1234, TimeUtils.parseDuration("1234 ms").toMillis());
    }

    @Test
    void testParseDurationSeconds() {
        Assertions.assertEquals(667766, TimeUtils.parseDuration("667766s").getSeconds());
        Assertions.assertEquals(667766, TimeUtils.parseDuration("667766sec").getSeconds());
        Assertions.assertEquals(667766, TimeUtils.parseDuration("667766secs").getSeconds());
        Assertions.assertEquals(667766, TimeUtils.parseDuration("667766second").getSeconds());
        Assertions.assertEquals(667766, TimeUtils.parseDuration("667766seconds").getSeconds());
        Assertions.assertEquals(667766, TimeUtils.parseDuration("667766 s").getSeconds());
    }

    @Test
    void testParseDurationMinutes() {
        Assertions.assertEquals(7657623, TimeUtils.parseDuration("7657623m").toMinutes());
        Assertions.assertEquals(7657623, TimeUtils.parseDuration("7657623min").toMinutes());
        Assertions.assertEquals(7657623, TimeUtils.parseDuration("7657623minute").toMinutes());
        Assertions.assertEquals(7657623, TimeUtils.parseDuration("7657623minutes").toMinutes());
        Assertions.assertEquals(7657623, TimeUtils.parseDuration("7657623 min").toMinutes());
    }

    @Test
    void testParseDurationHours() {
        Assertions.assertEquals(987654, TimeUtils.parseDuration("987654h").toHours());
        Assertions.assertEquals(987654, TimeUtils.parseDuration("987654hour").toHours());
        Assertions.assertEquals(987654, TimeUtils.parseDuration("987654hours").toHours());
        Assertions.assertEquals(987654, TimeUtils.parseDuration("987654 h").toHours());
    }

    @Test
    void testParseDurationDays() {
        Assertions.assertEquals(987654, TimeUtils.parseDuration("987654d").toDays());
        Assertions.assertEquals(987654, TimeUtils.parseDuration("987654day").toDays());
        Assertions.assertEquals(987654, TimeUtils.parseDuration("987654days").toDays());
        Assertions.assertEquals(987654, TimeUtils.parseDuration("987654 d").toDays());
    }

    @Test
    void testParseDurationUpperCase() {
        Assertions.assertEquals(1L, TimeUtils.parseDuration("1 NS").toNanos());
        Assertions.assertEquals(1000L, TimeUtils.parseDuration("1 MICRO").toNanos());
        Assertions.assertEquals(1L, TimeUtils.parseDuration("1 MS").toMillis());
        Assertions.assertEquals(1L, TimeUtils.parseDuration("1 S").getSeconds());
        Assertions.assertEquals(1L, TimeUtils.parseDuration("1 MIN").toMinutes());
        Assertions.assertEquals(1L, TimeUtils.parseDuration("1 H").toHours());
        Assertions.assertEquals(1L, TimeUtils.parseDuration("1 D").toDays());
    }

    @Test
    void testParseDurationTrim() {
        Assertions.assertEquals(155L, TimeUtils.parseDuration("      155      ").toMillis());
        Assertions.assertEquals(155L, TimeUtils.parseDuration("      155      ms   ").toMillis());
    }

    @Test
    void testParseDurationInvalid() {
        // null
        try {
            TimeUtils.parseDuration(null);
            Assertions.fail("exception expected");
        } catch (NullPointerException ignored) {
        }

        // empty
        try {
            TimeUtils.parseDuration("");
            Assertions.fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // blank
        try {
            TimeUtils.parseDuration("     ");
            Assertions.fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // no number
        try {
            TimeUtils.parseDuration("foobar or fubar or foo bazz");
            Assertions.fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // wrong unit
        try {
            TimeUtils.parseDuration("16 gjah");
            Assertions.fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // multiple numbers
        try {
            TimeUtils.parseDuration("16 16 17 18 ms");
            Assertions.fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }

        // negative number
        try {
            TimeUtils.parseDuration("-100 ms");
            Assertions.fail("exception expected");
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseDurationNumberOverflow() {
        TimeUtils.parseDuration("100000000000000000000000000000000 ms");
    }

    @Test
    void testGetStringInMillis() {
        Assertions.assertEquals("4567ms", TimeUtils.getStringInMillis(Duration.ofMillis(4567L)));
        Assertions.assertEquals(
                "4567000ms", TimeUtils.getStringInMillis(Duration.ofSeconds(4567L)));
        Assertions.assertEquals(
                "4ms", TimeUtils.getStringInMillis(Duration.of(4567L, ChronoUnit.MICROS)));
    }

    @Test
    void testToDuration() {
        final Time time = Time.of(1337, TimeUnit.MICROSECONDS);
        final Duration duration = TimeUtils.toDuration(time);

        MatcherAssert.assertThat(
                duration.toNanos(), is(equalTo(time.getUnit().toNanos(time.getSize()))));
    }
}
