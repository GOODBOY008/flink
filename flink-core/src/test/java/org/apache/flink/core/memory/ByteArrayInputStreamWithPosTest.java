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

import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link ByteArrayInputStreamWithPos}. */
public class ByteArrayInputStreamWithPosTest {

    @Rule public ExpectedException thrown = ExpectedException.none();

    private final byte[] data = new byte[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    private final ByteArrayInputStreamWithPos stream = new ByteArrayInputStreamWithPos(data);

    @Test
    void testGetWithNullArray() {
        thrown.expect(NullPointerException.class);
        stream.read(null, 0, 1);
    }

    @Test
    void testGetWithNegativeLength() {
        int read = stream.read(new byte[0], 0, -1);
        Assertions.assertEquals(0, read);
    }

    @Test
    void testGetWithTargetArrayOverflow() {
        thrown.expect(IndexOutOfBoundsException.class);
        stream.read(new byte[0], 0, 2);
    }

    @Test
    void testGetWithEOF() {
        drainStream(stream);
        int read = stream.read(new byte[1], 0, 1);
        Assertions.assertEquals(-1, read);
    }

    @Test
    void testGetMoreThanAvailable() {
        int read = stream.read(new byte[20], 0, 20);
        Assertions.assertEquals(10, read);
        Assertions.assertEquals(-1, stream.read()); // exhausted now
    }

    /** Test setting position on a {@link ByteArrayInputStreamWithPos}. */
    @Test
    void testSetPosition() {
        Assertions.assertEquals(data.length, stream.available());
        Assertions.assertEquals('0', stream.read());

        stream.setPosition(1);
        Assertions.assertEquals(data.length - 1, stream.available());
        Assertions.assertEquals('1', stream.read());

        stream.setPosition(3);
        Assertions.assertEquals(data.length - 3, stream.available());
        Assertions.assertEquals('3', stream.read());

        stream.setPosition(data.length);
        Assertions.assertEquals(0, stream.available());
        Assertions.assertEquals(-1, stream.read());
    }

    /** Test that the expected position exceeds the capacity of the byte array. */
    @Test
    void testSetTooLargePosition() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Position out of bounds.");
        stream.setPosition(data.length + 1);
    }

    /** Test setting a negative position. */
    @Test
    void testSetNegativePosition() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Position out of bounds.");
        stream.setPosition(-1);
    }

    @Test
    void testSetBuffer() {
        ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos();
        Assertions.assertEquals(-1, in.read());
        byte[] testData = new byte[] {0x42, 0x43, 0x44, 0x45};
        int off = 1;
        int len = 2;
        in.setBuffer(testData, off, len);
        for (int i = 0; i < len; ++i) {
            Assertions.assertEquals(testData[i + off], in.read());
        }
        Assertions.assertEquals(-1, in.read());
    }

    private static int drainStream(ByteArrayInputStreamWithPos stream) {
        int skipped = 0;
        while (stream.read() != -1) {
            skipped++;
        }
        return skipped;
    }
}
