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

package com.github.shyiko.mysql.binlog.io;

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Unit test for {@link BufferedSocketInputStream}. */
public class BufferedSocketInputStreamTest {

    @Test
    public void testReadFromBufferedSocketInputStream() throws Exception {
        BufferedSocketInputStream in =
                new BufferedSocketInputStream(
                        new ByteArrayInputStream(
                                new byte[] {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'}));
        byte[] buf = new byte[3];
        assertEquals(3, in.read(buf, 0, buf.length));
        assertTrue(Arrays.equals(new byte[] {'A', 'B', 'C'}, buf));
        assertEquals(5, in.available());

        assertEquals(3, in.read(buf, 0, buf.length));
        assertTrue(Arrays.equals(new byte[] {'D', 'E', 'F'}, buf));
        assertEquals(2, in.available());

        buf = new byte[2];
        assertEquals(2, in.read(buf, 0, buf.length));
        assertTrue(Arrays.equals(new byte[] {'G', 'H'}, buf));
        assertEquals(0, in.available());

        // reach the end of stream normally
        assertEquals(-1, in.read(buf, 0, buf.length));
        assertEquals(0, in.available());
    }

    @Test
    public void test() {
        float xx = (180L * 1.0f) / 398L;
        String xx1 = "binlog-000123";
        String xx2 = "binlog-000124";
        BinlogOffset aa = new BinlogOffset(xx1, 11141);
        BinlogOffset bb = new BinlogOffset(xx2, 11112);
        System.out.println(aa.isAfter(bb));
    }
}
