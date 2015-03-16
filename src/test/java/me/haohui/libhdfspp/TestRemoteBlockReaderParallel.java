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

import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;

import java.io.IOException;
import java.lang.Override;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestRemoteBlockReaderParallel extends TestRemoteBlockReaderCase {

  @Test
  public void testParallelReads() throws IOException, InterruptedException {
    final int numThreads = 4;
    final int numReaders = 7;

    final CountDownLatch latch = new CountDownLatch(numReaders);
    final AtomicBoolean testFailed = new AtomicBoolean(false);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    LocatedBlock lb = getFirstLocatedBlock();
    int readLength = 128;
    try (NativeIoService ioService = new NativeIoService()) {
      for (int i = 0; i < numReaders; i++) {
        int readOffset = i * BLOCK_SIZE / 8;
        executor.execute(new BlockerReaderRunnable(ioService, lb, latch, testFailed,
            readOffset, readLength));
      }
      latch.await();
      Assert.assertEquals(testFailed.get(), false);
    }
  }

  class BlockerReaderRunnable implements Runnable {
    private final NativeIoService ioService;
    private final LocatedBlock lb;
    private CountDownLatch latch;
    private AtomicBoolean bFail;
    private final int offset;
    private final int length;

    public BlockerReaderRunnable(NativeIoService ioService, LocatedBlock lb,
        CountDownLatch latch, AtomicBoolean bFail, int offset, int length) {
      this.ioService = ioService;
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
}