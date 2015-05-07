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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

class NativeTcpConnection implements Closeable {
  private final long handle;
  long handle() {
    return handle;
  }

  NativeTcpConnection(NativeIoService ioService) {
    handle = create(ioService.handle());
  }

  void connect(InetSocketAddress addr) throws IOException {
    byte[] status = connect(handle, addr.getAddress().getHostAddress(),
                            addr.getPort());
    NativeStatus stat = new NativeStatus(status);
    stat.checkForIOException();
  }

  void disconnect() {
    disconnect(handle);
  }

  @Override
  public void close() throws IOException {
    disconnect();
    destroy(handle);
  }

  private native static long create(long nativeIoService);
  private native static byte[] connect(long handle, String host, int port);
  private native static void disconnect(long handle);
  private native static void destroy(long handle);
}
