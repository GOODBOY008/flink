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

import org.apache.flink.configuration.ConfigConstants;

import org.junit.Rule;

import org.junit.jupiter.api.BeforeEach;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.AssertionsForClassTypes.within;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;

/** Tests for {@link ByteArrayOutputStreamWithPos}. */
public class ByteArrayOutputStreamWithPosTest {

    private static final int BUFFER_SIZE = 32;

    @Rule public ExpectedException thrown = ExpectedException.none();

    private ByteArrayOutputStreamWithPos stream;

    @BeforeEach
    public void setup() {
        stream = new ByteArrayOutputStreamWithPos(BUFFER_SIZE);
    }

    /** Test setting position which is exactly the same with the buffer size. */
    @Test
    void testSetPositionWhenBufferIsFull() throws Exception {
        stream.write(new byte[BUFFER_SIZE]);

        // check whether the buffer is filled fully
   assertThat(stream.getBuf().length).isEqualTo(BUFFER_SIZE);

        // check current position is the end of the buffer
   assertThat(stream.getPosition()).isEqualTo(BUFFER_SIZE);

        stream.setPosition(BUFFER_SIZE);

        // confirm current position is at where we expect.
   assertThat(stream.getPosition()).isEqualTo(BUFFER_SIZE);
    }

    /** Test setting negative position. */
    @Test
    void testSetNegativePosition() throws Exception {
        stream.write(new byte[BUFFER_SIZE]);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Position out of bounds");
        stream.setPosition(-1);
    }

    /** Test setting position larger than buffer size. */
    @Test
    void testSetPositionLargerThanBufferSize() throws Exception {
        // fully fill the buffer
        stream.write(new byte[BUFFER_SIZE]);
   assertThat(stream.getBuf().length).isEqualTo(BUFFER_SIZE);

        // expand the buffer by setting position beyond the buffer length
        stream.setPosition(BUFFER_SIZE + 1);
   assertThat(stream.getBuf().length).isEqualTo(BUFFER_SIZE * 2);
   assertThat(stream.getPosition()).isEqualTo(BUFFER_SIZE + 1);
    }

    /** Test that toString returns a substring of the buffer with range(0, position). */
    @Test
    void testToString() throws IOException {
        byte[] data = "1234567890".getBytes(ConfigConstants.DEFAULT_CHARSET);

        ByteArrayOutputStreamWithPos stream = new ByteArrayOutputStreamWithPos(data.length);

        stream.write(data);
   assertArrayEquals(
                data, stream.toString().getBytes(ConfigConstants.DEFAULT_CHARSET));

        for (int i = 0; i < data.length; i++) {
            stream.setPosition(i);
       assertArrayEquals(
                    Arrays.copyOf(data, i),
                    stream.toString().getBytes(ConfigConstants.DEFAULT_CHARSET));
        }

        // validate that the stored bytes are still tracked properly even when expanding array
        stream.setPosition(data.length + 1);
   assertArrayEquals(
                Arrays.copyOf(data, data.length + 1),
                stream.toString().getBytes(ConfigConstants.DEFAULT_CHARSET));
    }
}
