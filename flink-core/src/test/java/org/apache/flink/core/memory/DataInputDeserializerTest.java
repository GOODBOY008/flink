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


import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.AssertionsForClassTypes.within;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

/** Test suite for the {@link DataInputDeserializer} class. */
public class DataInputDeserializerTest {

    @Test
    void testAvailable() throws Exception {
        byte[] bytes;
        DataInputDeserializer dis;

        bytes = new byte[] {};
        dis = new DataInputDeserializer(bytes, 0, bytes.length);
   assertThat(dis.available()).isEqualTo(bytes.length);

        bytes = new byte[] {1, 2, 3};
        dis = new DataInputDeserializer(bytes, 0, bytes.length);
   assertThat(dis.available()).isEqualTo(bytes.length);

        dis.readByte();
   assertThat(dis.available()).isEqualTo(2);
        dis.readByte();
   assertThat(dis.available()).isEqualTo(1);
        dis.readByte();
   assertThat(dis.available()).isEqualTo(0);

        try {
            dis.readByte();
       fail("Did not throw expected IOException");
        } catch (IOException e) {
            // ignore
        }
   assertThat(dis.available()).isEqualTo(0);
    }

    @Test
    void testReadWithLenZero() throws IOException {
        byte[] bytes = new byte[0];
        DataInputDeserializer dis = new DataInputDeserializer(bytes, 0, bytes.length);
   assertThat(dis.available()).isEqualTo(0);

        byte[] bytesForRead = new byte[0];
   assertEquals(
                0, dis.read(bytesForRead, 0, 0)); // do not throw when read with len 0
    }
}
