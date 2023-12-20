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

package org.apache.flink.types.parser;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** */
public abstract class ParserTestBase<T> extends TestLogger {

    public abstract String[] getValidTestValues();

    public abstract T[] getValidTestResults();

    public abstract String[] getInvalidTestValues();

    public abstract boolean allowsEmptyField();

    public abstract FieldParser<T> getParser();

    public abstract Class<T> getTypeClass();

    @Test
    void testTest() {
        Assertions.assertNotNull(getParser());
        Assertions.assertNotNull(getTypeClass());
        Assertions.assertNotNull(getValidTestValues());
        Assertions.assertNotNull(getValidTestResults());
        Assertions.assertNotNull(getInvalidTestValues());
        Assertions.assertEquals(getValidTestValues().length, getValidTestResults().length);
    }

    @Test
    void testGetValue() {
        try {
            FieldParser<?> parser = getParser();
            Object created = parser.createValue();

            Assertions.assertNotNull(created, "Null type created");
            Assertions.assertTrue(
                    getTypeClass().isAssignableFrom(created.getClass()), "Wrong type created");
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assertions.fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    void testValidStringInIsolation() {
        try {
            String[] testValues = getValidTestValues();
            T[] results = getValidTestResults();

            for (int i = 0; i < testValues.length; i++) {

                FieldParser<T> parser1 = getParser();
                FieldParser<T> parser2 = getParser();
                FieldParser<T> parser3 = getParser();

                byte[] bytes1 = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);
                byte[] bytes2 = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);
                byte[] bytes3 = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);

                int numRead1 =
                        parser1.parseField(
                                bytes1, 0, bytes1.length, new byte[] {'|'}, parser1.createValue());
                int numRead2 =
                        parser2.parseField(
                                bytes2,
                                0,
                                bytes2.length,
                                new byte[] {'&', '&'},
                                parser2.createValue());
                int numRead3 =
                        parser3.parseField(
                                bytes3,
                                0,
                                bytes3.length,
                                new byte[] {'9', '9', '9'},
                                parser3.createValue());

                Assertions.assertTrue(
                        numRead1 != -1,
                        "Parser declared the valid value " + testValues[i] + " as invalid.");
                Assertions.assertTrue(
                        numRead2 != -1,
                        "Parser declared the valid value " + testValues[i] + " as invalid.");
                Assertions.assertTrue(
                        numRead3 != -1,
                        "Parser declared the valid value " + testValues[i] + " as invalid.");

                Assertions.assertEquals(
                        bytes1.length, numRead1, "Invalid number of bytes read returned.");
                Assertions.assertEquals(
                        bytes2.length, numRead2, "Invalid number of bytes read returned.");
                Assertions.assertEquals(
                        bytes3.length, numRead3, "Invalid number of bytes read returned.");

                T result1 = parser1.getLastResult();
                T result2 = parser2.getLastResult();
                T result3 = parser3.getLastResult();

                Assertions.assertEquals(
                        results[i], result1, "Parser parsed wrong. " + testValues[i]);
                Assertions.assertEquals(
                        results[i], result2, "Parser parsed wrong. " + testValues[i]);
                Assertions.assertEquals(
                        results[i], result3, "Parser parsed wrong. " + testValues[i]);
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assertions.fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    void testValidStringInIsolationWithEndDelimiter() {
        try {
            String[] testValues = getValidTestValues();
            T[] results = getValidTestResults();

            for (int i = 0; i < testValues.length; i++) {

                FieldParser<T> parser1 = getParser();
                FieldParser<T> parser2 = getParser();

                String testVal1 = testValues[i] + "|";
                String testVal2 = testValues[i] + "&&&&";

                byte[] bytes1 = testVal1.getBytes(ConfigConstants.DEFAULT_CHARSET);
                byte[] bytes2 = testVal2.getBytes(ConfigConstants.DEFAULT_CHARSET);

                int numRead1 =
                        parser1.parseField(
                                bytes1, 0, bytes1.length, new byte[] {'|'}, parser1.createValue());
                int numRead2 =
                        parser2.parseField(
                                bytes2,
                                0,
                                bytes2.length,
                                new byte[] {'&', '&', '&', '&'},
                                parser2.createValue());

                Assertions.assertTrue(
                        numRead1 != -1,
                        "Parser declared the valid value " + testValues[i] + " as invalid.");
                Assertions.assertTrue(
                        numRead2 != -1,
                        "Parser declared the valid value " + testValues[i] + " as invalid.");

                Assertions.assertEquals(
                        bytes1.length, numRead1, "Invalid number of bytes read returned.");
                Assertions.assertEquals(
                        bytes2.length, numRead2, "Invalid number of bytes read returned.");

                T result1 = parser1.getLastResult();
                T result2 = parser2.getLastResult();

                Assertions.assertEquals(results[i], result1, "Parser parsed wrong.");
                Assertions.assertEquals(results[i], result2, "Parser parsed wrong.");
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assertions.fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    void testConcatenated() {
        try {
            String[] testValues = getValidTestValues();
            T[] results = getValidTestResults();
            byte[] allBytesWithDelimiter = concatenate(testValues, new char[] {'|'}, true);
            byte[] allBytesNoDelimiterEnd = concatenate(testValues, new char[] {','}, false);

            FieldParser<T> parser1 = getParser();
            FieldParser<T> parser2 = getParser();

            T val1 = parser1.createValue();
            T val2 = parser2.createValue();

            int pos1 = 0;
            int pos2 = 0;

            for (int i = 0; i < results.length; i++) {
                pos1 =
                        parser1.parseField(
                                allBytesWithDelimiter,
                                pos1,
                                allBytesWithDelimiter.length,
                                new byte[] {'|'},
                                val1);
                pos2 =
                        parser2.parseField(
                                allBytesNoDelimiterEnd,
                                pos2,
                                allBytesNoDelimiterEnd.length,
                                new byte[] {','},
                                val2);

                Assertions.assertTrue(
                        pos1 != -1,
                        "Parser declared the valid value " + testValues[i] + " as invalid.");
                Assertions.assertTrue(
                        pos2 != -1,
                        "Parser declared the valid value " + testValues[i] + " as invalid.");

                T result1 = parser1.getLastResult();
                T result2 = parser2.getLastResult();

                Assertions.assertEquals(results[i], result1, "Parser parsed wrong.");
                Assertions.assertEquals(results[i], result2, "Parser parsed wrong.");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assertions.fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    void testConcatenatedMultiCharDelimiter() {
        try {
            String[] testValues = getValidTestValues();
            T[] results = getValidTestResults();
            byte[] allBytesWithDelimiter =
                    concatenate(testValues, new char[] {'&', '&', '&', '&'}, true);
            byte[] allBytesNoDelimiterEnd =
                    concatenate(testValues, new char[] {'9', '9', '9'}, false);

            FieldParser<T> parser1 = getParser();
            FieldParser<T> parser2 = getParser();

            T val1 = parser1.createValue();
            T val2 = parser2.createValue();

            int pos1 = 0;
            int pos2 = 0;

            for (int i = 0; i < results.length; i++) {
                pos1 =
                        parser1.parseField(
                                allBytesWithDelimiter,
                                pos1,
                                allBytesWithDelimiter.length,
                                new byte[] {'&', '&', '&', '&'},
                                val1);
                Assertions.assertTrue(
                        pos1 != -1,
                        "Parser declared the valid value " + testValues[i] + " as invalid.");
                T result1 = parser1.getLastResult();
                Assertions.assertEquals(results[i], result1, "Parser parsed wrong.");

                pos2 =
                        parser2.parseField(
                                allBytesNoDelimiterEnd,
                                pos2,
                                allBytesNoDelimiterEnd.length,
                                new byte[] {'9', '9', '9'},
                                val2);
                Assertions.assertTrue(
                        pos2 != -1,
                        "Parser declared the valid value " + testValues[i] + " as invalid.");
                T result2 = parser2.getLastResult();
                Assertions.assertEquals(results[i], result2, "Parser parsed wrong.");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assertions.fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    void testInValidStringInIsolation() {
        try {
            String[] testValues = getInvalidTestValues();

          for (String testValue : testValues) {

            FieldParser<T> parser = getParser();

            byte[] bytes = testValue.getBytes(ConfigConstants.DEFAULT_CHARSET);
            int numRead =
                parser.parseField(
                    bytes, 0, bytes.length, new byte[] {'|'}, parser.createValue());

            Assertions.assertEquals(
                -1, numRead, "Parser accepted the invalid value " + testValue + ".");
          }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assertions.fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    void testInValidStringsMixedIn() {
        try {
            String[] validValues = getValidTestValues();
            T[] validResults = getValidTestResults();

            String[] invalidTestValues = getInvalidTestValues();

            FieldParser<T> parser = getParser();
            T value = parser.createValue();

            for (String invalid : invalidTestValues) {

                // place an invalid string in the middle
                String[] testLine = new String[validValues.length + 1];
                int splitPoint = validValues.length / 2;
                System.arraycopy(validValues, 0, testLine, 0, splitPoint);
                testLine[splitPoint] = invalid;
                System.arraycopy(
                        validValues,
                        splitPoint,
                        testLine,
                        splitPoint + 1,
                        validValues.length - splitPoint);

                byte[] bytes = concatenate(testLine, new char[] {'%'}, true);

                // read the valid parts
                int pos = 0;
                for (int i = 0; i < splitPoint; i++) {
                    pos = parser.parseField(bytes, pos, bytes.length, new byte[] {'%'}, value);

                    Assertions.assertTrue(
                            pos != -1,
                            "Parser declared the valid value " + validValues[i] + " as invalid.");
                    T result = parser.getLastResult();
                    Assertions.assertEquals(validResults[i], result, "Parser parsed wrong.");
                }

                // fail on the invalid part
                pos = parser.parseField(bytes, pos, bytes.length, new byte[] {'%'}, value);
                Assertions.assertEquals(
                        -1, pos, "Parser accepted the invalid value " + invalid + ".");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assertions.fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStaticParseMethod() {
        try {
            Method parseMethod = null;
            try {
                parseMethod =
                        getParser()
                                .getClass()
                                .getMethod(
                                        "parseField",
                                        byte[].class,
                                        int.class,
                                        int.class,
                                        char.class);
            } catch (NoSuchMethodException e) {
                return;
            }

            String[] testValues = getValidTestValues();
            T[] results = getValidTestResults();

            for (int i = 0; i < testValues.length; i++) {

                byte[] bytes = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);

                T result;
                try {
                    result = (T) parseMethod.invoke(null, bytes, 0, bytes.length, '|');
                } catch (InvocationTargetException e) {
                    e.getTargetException().printStackTrace();
                    Assertions.fail("Error while parsing: " + e.getTargetException().getMessage());
                    return;
                }
                Assertions.assertEquals(results[i], result, "Parser parsed wrong.");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assertions.fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    void testStaticParseMethodWithInvalidValues() {
        try {
            Method parseMethod = null;
            try {
                parseMethod =
                        getParser()
                                .getClass()
                                .getMethod(
                                        "parseField",
                                        byte[].class,
                                        int.class,
                                        int.class,
                                        char.class);
            } catch (NoSuchMethodException e) {
                return;
            }

            String[] testValues = getInvalidTestValues();

          for (String testValue : testValues) {

            byte[] bytes = testValue.getBytes(ConfigConstants.DEFAULT_CHARSET);

            try {
              parseMethod.invoke(null, bytes, 0, bytes.length, '|');
              Assertions.fail("Static parse method accepted invalid value");
            } catch (InvocationTargetException e) {
              // that's it!
            }
          }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assertions.fail("Test erroneous: " + e.getMessage());
        }
    }

    private static byte[] concatenate(String[] values, char[] delimiter, boolean delimiterAtEnd) {
        int len = 0;
        for (String s : values) {
            len += s.length() + delimiter.length;
        }

        if (!delimiterAtEnd) {
            len -= delimiter.length;
        }

        int currPos = 0;
        byte[] result = new byte[len];

        for (int i = 0; i < values.length; i++) {
            String s = values[i];

            byte[] bytes = s.getBytes(ConfigConstants.DEFAULT_CHARSET);
            int numBytes = bytes.length;
            System.arraycopy(bytes, 0, result, currPos, numBytes);
            currPos += numBytes;

            if (delimiterAtEnd || i < values.length - 1) {
              for (char c : delimiter)
                result[currPos++] = (byte) c;
            }
        }

        return result;
    }

    @Test
    void testTrailingEmptyField() {
        try {
            FieldParser<T> parser = getParser();

            byte[] bytes = "||".getBytes(ConfigConstants.DEFAULT_CHARSET);

            for (int i = 0; i < 2; i++) {

                // test empty field with trailing delimiter when i = 0,
                // test empty field with out trailing delimiter when i= 1.
                int numRead =
                        parser.parseField(
                                bytes, i, bytes.length, new byte[] {'|'}, parser.createValue());

                Assertions.assertEquals(
                        FieldParser.ParseErrorState.EMPTY_COLUMN, parser.getErrorState());

                if (this.allowsEmptyField()) {
                    Assertions.assertTrue(
                            numRead != -1, "Parser declared the empty string as invalid.");
                    Assertions.assertEquals(
                            i + 1, numRead, "Invalid number of bytes read returned.");
                } else {
                    Assertions.assertEquals(-1, numRead, "Parser accepted the empty string.");
                }

                parser.resetParserState();
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assertions.fail("Test erroneous: " + e.getMessage());
        }
    }
}
