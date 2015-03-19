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

import org.apache.commons.io.Charsets;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;

class NativeStatus {
  enum StateCode {
    K_OK(0),
    K_INVALID_ARGUMENT(22), // EINVAL
    K_GENERIC_ERROR(1),
    K_INVALID_ENCRYPTION_KEY(2),
    K_UNIMPLEMENTED(3),
    K_RPC_CONNECTION_RESET(54), // ECONNRESET
    K_RPC_TIMEOUT(60), // ETIMEDOUT
    K_EXCEPTION(256);

    private final int value;
    StateCode(int value) {
      this.value = value;
    }
  }
  private final byte[] state;

  NativeStatus(byte[] state) {
    this.state = state;
  }

  boolean ok() {
    return state == null;
  }

  private int code() {
    if (ok()) {
      return 0;
    } else {
      return ByteBuffer.wrap(state, 0, 8).getInt();
    }
  }

  private String message() {
    if (ok()) {
      return "OK";
    } else {
      return new String(state, 8, state.length - 8, Charsets.UTF_8);
    }
  }

  public void checkForIOException() throws IOException {
    if (!ok()) {
      if (code() == StateCode.K_RPC_TIMEOUT.value) {
        throw new SocketTimeoutException(message());
      } else if (code() == StateCode.K_RPC_CONNECTION_RESET.value) {
        throw new ConnectException(message());
      } else {
        throw new IOException(message());
      }
    }
  }
}
