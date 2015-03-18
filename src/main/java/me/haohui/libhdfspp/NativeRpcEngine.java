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

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import org.apache.commons.io.Charsets;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

class NativeRpcEngine implements Closeable {
  private final long handle;

  NativeRpcEngine(NativeIoService ioService, byte[] clientName, String
      protocol, int version) {
    handle = create(ioService.handle(), clientName, protocol.getBytes
        (Charsets.UTF_8), version);
  }

  void connect(InetSocketAddress addr) throws IOException {
    byte[] status = connect(handle, addr.getAddress().getHostAddress(),
                            addr.getPort());
    NativeStatus stat = new NativeStatus(status);
    stat.checkForIOException();
  }

  void startReadLoop() {
    startReadLoop(handle);
  }

  NativeStatus rpc(byte[] method, MessageLite request,
      MessageLite.Builder response) throws IOException {
    NativeStatus status = new NativeStatus(null);
    byte[][] stat = new byte[1][];
    byte[] resp = rpc(handle, method, request.toByteArray(), stat);
    if (stat[0] != null) {
      status = new NativeStatus(stat[0]);
    }
    response.clear().mergeFrom(resp);
    return status;
  }

  @Override
  public void close() throws IOException {
    destroy(handle);
  }

  private native static long create(long ioService, byte[] clientName, byte[]
      protocol, int version);
  private native static byte[] connect(long handle, String host, int port);
  private native static void startReadLoop(long handle);
  private native static byte[] rpc(long handle, byte[] method, byte[]
      request, byte[][] status);
  private native static void destroy(long handle);
}
