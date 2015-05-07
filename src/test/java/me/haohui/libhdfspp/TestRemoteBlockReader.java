/**
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
package me.haohui.libhdfspp;

import com.google.common.base.Charsets;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class TestRemoteBlockReader {
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;
  private static final String FILENAME = "/foo";
  private static byte[] CONTENTS;
  private static final int BLOCK_SIZE = 1024;
  private static final int CONTENT_SIZE = 2 * BLOCK_SIZE;
  private static final byte[] CLIENT_NAME =
      "libhdfs++".getBytes(Charsets.UTF_8);

  @BeforeClass
  public static void setUp() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

    byte[] chars = generateCharacterTable();
    CONTENTS = new byte[CONTENT_SIZE];
    for (int i = 0; i < CONTENT_SIZE; ++i) {
      CONTENTS[i] = chars[i % chars.length];
    }

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    fs = cluster.getFileSystem();
    OutputStream os = fs.create(new Path(FILENAME));
    os.write(CONTENTS);
    os.close();
  }

  @AfterClass
  public static void tearDown() throws InterruptedException, IOException {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Rule
  public Timeout timeout = new Timeout(300000);

  @Test
  public void testReadWholeBlock() throws IOException, InterruptedException {
    int readLength = BLOCK_SIZE;
    int readOffset = 0;
    LocatedBlock lb = getFirstLocatedBlock();
    testReadBlockCase(lb, readOffset, readLength);
  }

  @Test
  public void testReadAtChecksumBoundary() throws IOException, InterruptedException {
    //Test whether it can read from the middle of the checksum chunk (512)
    int readLength = BLOCK_SIZE / 4;
    int readOffset = BLOCK_SIZE / 8;
    LocatedBlock lb = getFirstLocatedBlock();
    testReadBlockCase(lb, readOffset, readLength);
  }

  @Test
  public void testReadFromOffsetZero() throws IOException, InterruptedException {
    int readLength = BLOCK_SIZE - 1;
    int readOffset = 0;
    LocatedBlock lb = getFirstLocatedBlock();
    testReadBlockCase(lb, readOffset, readLength);
  }

  @Test
  public void testReadZeroByte() throws IOException, InterruptedException {
    int readLength = 0;
    int readOffset = 0;
    LocatedBlock lb = getFirstLocatedBlock();
    testReadBlockCase(lb, readOffset, readLength);
  }

  @Test
  public void testReadOneByte() throws IOException, InterruptedException {
    int readLength = 0;
    int readOffset = 1;
    LocatedBlock lb = getFirstLocatedBlock();
    testReadBlockCase(lb, readOffset, readLength);
  }

  private void testReadBlockCase(
      LocatedBlock lb, int readOffset, int readLength)
      throws IOException, InterruptedException {
    ExtendedBlock eb = lb.getBlock();
    try (NativeIoService ioService = new NativeIoService();
         NativeTcpConnection conn = new NativeTcpConnection(ioService);
         IoServiceExecutor executor = new IoServiceExecutor(ioService)
    ) {
      executor.start();
      conn.connect(cluster.getDataNodes().get(0).getXferAddress());
      try (NativeRemoteBlockReader reader = new NativeRemoteBlockReader
          (conn)) {
        reader.connect(CLIENT_NAME, null, eb, readLength, readOffset);
        ByteBuffer buf = ByteBuffer.allocateDirect(readLength);
        int transferred = reader.read(buf);
        assertEquals(readLength, transferred);
        buf.flip();
        byte[] data = new byte[readLength];
        buf.get(data);
        byte[] origData = new byte[readLength];
        System.arraycopy(CONTENTS, readOffset, origData, 0, readLength);
        assertArrayEquals(origData, data);
      }
    }
  }

  private LocatedBlock getFirstLocatedBlock() throws IOException {
    BlockLocation[] locs = fs.getFileBlockLocations(new Path(FILENAME), 0,
                                                    CONTENT_SIZE);
    return ((HdfsBlockLocation) locs[0]).getLocatedBlock();
  }

  private static byte[] generateCharacterTable() {
    byte[] chars = new byte[64];
    for (int i = 0; i < 10; ++i) {
      chars[i] = (byte) ('0' + i);
    }
    for (int i = 0; i < 26; ++i) {
      chars[i + 10] = (byte) ('a' + i);
    }
    for (int i = 0; i < 26; ++i) {
      chars[i + 36] = (byte) ('A' + i);
    }
    chars[62] = '/';
    chars[63] = '+';
    return chars;
  }

  @Test
  public void testReadIntoChecksumChunk() throws IOException, InterruptedException {
    int readLength = BLOCK_SIZE / 4;
    int readOffset = 11;
    LocatedBlock lb = getFirstLocatedBlock();
    testReadBlockCase(lb, readOffset, readLength);
  }

}
