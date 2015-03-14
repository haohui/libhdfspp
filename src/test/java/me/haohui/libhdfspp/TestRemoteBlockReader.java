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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Charsets;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReader;
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
import org.junit.Ignore;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.Exception;
import java.lang.Override;
import java.lang.Runnable;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestRemoteBlockReader {
  private static HdfsConfiguration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;
  private static final String FILENAME = "/foo";
  private static byte[] CONTENTS;
  private static final int BLOCK_SIZE = 1024;
  private static final int CONTENT_SIZE = 2 * BLOCK_SIZE;
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
    int readLength = BLOCK_SIZE;
    int readOffset = 0;
    LocatedBlock lb = getFirstLocatedBlock();
    testReadBlockCase(lb, readOffset, readLength);
  }

  //Test whether it can read from the middle of the checksum chunk (512)
  @Test
  public void testReadFromMiddle1() throws IOException, InterruptedException {
    int readLength = BLOCK_SIZE/4;
    int readOffset = BLOCK_SIZE/8;
    LocatedBlock lb = getFirstLocatedBlock();
    testReadBlockCase(lb, readOffset, readLength);
  }

  @Test
  public void testReadFromMiddle2() throws IOException, InterruptedException {
    int readLength = BLOCK_SIZE/4 - 1;
    int readOffset = (BLOCK_SIZE * 3)/4;
    LocatedBlock lb = getFirstLocatedBlock();
    testReadBlockCase(lb, readOffset, readLength);
  }

  @Test
  public void testReadBoundary1() throws IOException, InterruptedException {
    int readLength = BLOCK_SIZE - 1;
    int readOffset = 0;
    LocatedBlock lb = getFirstLocatedBlock();
    testReadBlockCase(lb, readOffset, readLength);
  }

  @Test
  public void testReadBoundary2() throws IOException, InterruptedException {
    int readLength = BLOCK_SIZE - 2;
    int readOffset = 1;
    LocatedBlock lb = getFirstLocatedBlock();
    testReadBlockCase(lb, readOffset, readLength);
  }

  @Test
  @Ignore
  // Currently assert in protobuf_util.h ReadPBMessageMonad.CompletionHandler
  // We should have parameter check
  public void testReadBoundary3() throws IOException, InterruptedException {
    int readLength = BLOCK_SIZE - 2;
    int readOffset = -1;
    LocatedBlock lb = getFirstLocatedBlock();
    try {
      testReadBlockCase(lb, readOffset, readLength);
      fail("Should get exception");
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Test
  @Ignore
  // Currently assert in protobuf_util.h ReadPBMessageMonad.CompletionHandler
  // We should have parameter check
  public void testReadBoundary4() throws IOException, InterruptedException {
    int readLength = BLOCK_SIZE + 1;
    int readOffset = 0;
    LocatedBlock lb = getFirstLocatedBlock();
    try {
      testReadBlockCase(lb, readOffset, readLength);
      fail("Should get exception");
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Test
  public void testParallelReads() throws IOException, InterruptedException {
    final int numThreads = 4;
    final int numReaders = 7;

    final CountDownLatch latch = new CountDownLatch(numReaders);
    final AtomicBoolean testFailed = new AtomicBoolean(false);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    LocatedBlock lb = getFirstLocatedBlock();
    int readLength = 128;
    for (int i = 0; i < numReaders; i++) {
      int readOffset = i * BLOCK_SIZE / 8;
      executor.execute(new BlockerReaderRunnable(lb, latch, testFailed,
          readOffset, readLength));
    }
    latch.await();
    Assert.assertEquals(testFailed.get(), false);
  }

  class BlockerReaderRunnable implements Runnable {
    private final LocatedBlock lb;
    private CountDownLatch latch;
    private AtomicBoolean bFail;
    private int offset;
    private int length;

    public BlockerReaderRunnable(LocatedBlock lb, CountDownLatch latch,
        AtomicBoolean bFail, int offset, int length) {
      this.lb = lb;
      this.latch = latch;
      this.bFail = bFail;
      this.offset = offset;
      this.length = length;
    }

    @Override
    public void run() {
      try {
        for (int i = 0 ; i < 10; i++) {
          testReadBlockCase(lb, offset, length);
        }
      } catch (Exception e) {
        e.printStackTrace();
        bFail.set(true);
      } finally {
        latch.countDown();
      }
    }
  }

  private void testReadBlockCase(LocatedBlock lb, int readOffset, int readLength)
      throws IOException, InterruptedException {
    ExtendedBlock eb = lb.getBlock();
    try (NativeIoService ioService = new NativeIoService();
         IoServiceExecutor executor = new IoServiceExecutor(ioService);
         NativeTcpConnection conn = new NativeTcpConnection(ioService)
    ) {
      executor.start();
      conn.connect(cluster.getDataNodes().get(0).getXferAddress());
      try (NativeRemoteBlockReader reader = new NativeRemoteBlockReader
          (conn)) {
        reader.connect(CLIENT_NAME, null, eb, readLength, readOffset);
        ByteBuffer buf = ByteBuffer.allocateDirect(readLength);
        int transferred = reader.read(buf);
        Assert.assertEquals(readLength, transferred);
        buf.position(buf.position() + transferred);
        buf.flip();
        byte[] data = new byte[readLength];
        buf.get(data);
        byte[] origData = new byte[readLength];
        System.arraycopy(CONTENTS, readOffset, origData, 0, readLength);
        Assert.assertArrayEquals(origData, data);
      }
    }
  }

  private LocatedBlock getFirstLocatedBlock() throws IOException {
    BlockLocation[] locs = fs.getFileBlockLocations(new Path(FILENAME), 0,
        CONTENT_SIZE);
    return ((HdfsBlockLocation) locs[0]).getLocatedBlock();
  }
}
