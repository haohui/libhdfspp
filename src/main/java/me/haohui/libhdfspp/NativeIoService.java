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
import java.lang.Exception;
import java.lang.Override;

class NativeIoService implements Closeable {
  private final long handle;
  long handle() {
    return handle;
  }

  NativeIoService() {
    handle = create();
  }

  void run() {
    nativeRun(handle);
  }
  void stop() {
    stop(handle);
  }

  @Override
  public void close() throws IOException {
    stop(handle);
    destroy(handle);
  }

  private native static long create();
  private native static void nativeRun(long handle);
  private native static void stop(long handle);
  private native static void destroy(long handle);
}