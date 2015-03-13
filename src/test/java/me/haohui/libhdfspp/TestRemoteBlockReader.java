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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class TestRemoteBlockReader {
  private static HdfsConfiguration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;
  private static final String FILENAME = "/foo";
  private static byte[] CONTENTS;
  private static final int BLOCK_SIZE = 1024;
  private static final int CONTENT_SIZE = 1 * BLOCK_SIZE;
  private static final byte[] CLIENT_NAME = "libhdfs++".getBytes(Charsets
                                                                     .UTF_8);
  @BeforeClass
  public static void setUp() throws IOException {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

    byte[] chars = GetCharacterTable();
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

  private static byte[] GetCharacterTable() {
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

  @AfterClass
  public static void tearDown() throws InterruptedException {
    if (cluster != null) {
      cluster.shutdown();
    }

  }

  @Test
  public void testReadWholeBlock() throws IOException, InterruptedException {
    BlockLocation[] locs = fs.getFileBlockLocations(new Path(FILENAME), 0,
                                                    CONTENT_SIZE);
    LocatedBlock lb = ((HdfsBlockLocation) locs[0]).getLocatedBlock();
    ExtendedBlock eb = lb.getBlock();
    try (NativeIoService ioService = new NativeIoService();
         IoServiceExecutor executor = new IoServiceExecutor(ioService);
         NativeTcpConnection conn = new NativeTcpConnection(ioService)
    ) {
      executor.start();
      conn.connect(cluster.getDataNodes().get(0).getXferAddress());
      try (NativeRemoteBlockReader reader = new NativeRemoteBlockReader
          (conn)) {
        reader.connect(CLIENT_NAME, null, eb, BLOCK_SIZE, 0);
        ByteBuffer buf = ByteBuffer.allocateDirect(BLOCK_SIZE);
        int transferred = reader.read(buf);
        Assert.assertEquals(BLOCK_SIZE, transferred);
        buf.position(buf.position() + transferred);
        buf.flip();
        byte[] data = new byte[BLOCK_SIZE];
        buf.get(data);
        byte[] origData = new byte[BLOCK_SIZE];
        System.arraycopy(CONTENTS, 0, origData, 0, origData.length);
        Assert.assertArrayEquals(origData, data);
      }
    }
  }
}
