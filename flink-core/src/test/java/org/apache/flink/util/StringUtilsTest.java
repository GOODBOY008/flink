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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.DayOfWeek;
import java.util.Random;

/** Tests for the {@link StringUtils}. */
class StringUtilsTest extends TestLogger {

    @Test
    void testControlCharacters() {
        String testString = "\b \t \n \f \r default";
        String controlString = StringUtils.showControlCharacters(testString);
        Assertions.assertEquals("\\b \\t \\n \\f \\r default", controlString);
    }

    @Test
    void testArrayAwareToString() {
        Assertions.assertEquals("null", StringUtils.arrayAwareToString(null));

        Assertions.assertEquals("MONDAY", StringUtils.arrayAwareToString(DayOfWeek.MONDAY));

        Assertions.assertEquals("[1, 2, 3]", StringUtils.arrayAwareToString(new int[] {1, 2, 3}));

        Assertions.assertEquals(
                "[[4, 5, 6], null, []]",
                StringUtils.arrayAwareToString(new byte[][] {{4, 5, 6}, null, {}}));

        Assertions.assertEquals(
                "[[4, 5, 6], null, MONDAY]",
                StringUtils.arrayAwareToString(
                        new Object[] {new Integer[] {4, 5, 6}, null, DayOfWeek.MONDAY}));
    }

    @Test
    void testStringToHexArray() {
        String hex = "019f314a";
        byte[] hexArray = StringUtils.hexStringToByte(hex);
        byte[] expectedArray = new byte[] {1, -97, 49, 74};
        Assertions.assertArrayEquals(expectedArray, hexArray);
    }

    @Test
    void testHexArrayToString() {
        byte[] byteArray = new byte[] {1, -97, 49, 74};
        String hex = StringUtils.byteToHexString(byteArray);
        Assertions.assertEquals("019f314a", hex);
    }

    @Test
    void testGenerateAlphanumeric() {
        String str = StringUtils.generateRandomAlphanumericString(new Random(), 256);

        if (!str.matches("[a-zA-Z0-9]{256}")) {
            Assertions.fail("Not alphanumeric: " + str);
        }
    }
}
