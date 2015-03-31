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

public class TestRemoteBlockReader extends TestRemoteBlockReaderCase {
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
    int readLength = BLOCK_SIZE/4;
    int readOffset = BLOCK_SIZE/8;
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

  @Test
  public void testReadIntoChecksumChunk() throws IOException, InterruptedException {
    int readLength = BLOCK_SIZE/4;
    int readOffset = 11;            //offset to middle of checksum chunk, 11 is arbitrarily chosen because it is very unlikely to be the size of a chunk.
    LocatedBlock lb = getFirstLocatedBlock();
    testReadBlockCase(lb, readOffset, readLength);
  }

}
