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
import org.apache.commons.lang.NotImplementedException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.util.HashMap;

class NativeStatus {
  private static final HashMap<Integer, StateCode> statMap 
      = new HashMap<Integer, StateCode>();

  enum StateCode {
    OK(0, null),
    GENERIC_ERROR(1, IOException.class),
    INVALID_ENCRYPTION_KEY(2, InvalidKeyException.class),
    UNIMPLEMENTED(3, NotImplementedException.class),
    EINVAL(22, IllegalArgumentException.class), // Invalid argument
    EAGAIN(35, FileNotFoundException.class), // Resource temporarily unavailable
    ECONNRESET(54, ConnectException.class), // Connection reset by peer
    ETIMEDOUT(60, SocketTimeoutException.class), // Operation timed out
    NULL(255, Exception.class),
    EXCEPTION(256, Exception.class);

    static {
      statMap.put(StateCode.OK.value, StateCode.OK);
      statMap.put(StateCode.GENERIC_ERROR.value, StateCode.GENERIC_ERROR);
      statMap.put(StateCode.INVALID_ENCRYPTION_KEY.value, StateCode.INVALID_ENCRYPTION_KEY);
      statMap.put(StateCode.UNIMPLEMENTED.value, StateCode.UNIMPLEMENTED);
      statMap.put(StateCode.EINVAL.value, StateCode.EINVAL);
      statMap.put(StateCode.EAGAIN.value, StateCode.EAGAIN);
      statMap.put(StateCode.ECONNRESET.value, StateCode.ECONNRESET);
      statMap.put(StateCode.ETIMEDOUT.value, StateCode.ETIMEDOUT);
      statMap.put(StateCode.NULL.value, StateCode.NULL);
      statMap.put(StateCode.EXCEPTION.value, StateCode.EXCEPTION);
    }
    
    private final int value;
    private final Class<? extends Exception> exceptionClass;
    StateCode(int value, Class<? extends Exception> exceptionClass) {
      this.value = value;
      this.exceptionClass = exceptionClass;
    }
  }

  private final byte[] state;

  NativeStatus(byte[] state) {
    this.state = state;
  }

  boolean ok() {
    return state == null;
  }

  private StateCode code() {
    if (ok()) {
      return StateCode.OK;
    } else {
      int nativeCode = ByteBuffer.wrap(state, 0, 8).getInt();
      if (statMap.containsKey(nativeCode)) {
        return statMap.get(nativeCode);
      } else {
        return StateCode.NULL;
      }
    }
  }

  private String message() {
    if (ok()) {
      return "OK";
    } else {
      return String.format("(%d): %s", ByteBuffer.wrap(state, 0, 8).getInt(),
          new String(state, 8, state.length - 8, Charsets.UTF_8));
    }
  }

  public void checkForIOException() throws IOException {
    if (!ok()) {
      try {
        throw code().exceptionClass.getConstructor(String.class).newInstance(
            message());
      } catch (Exception e) {
        IOException ioe = new IOException();
        ioe.initCause(e.getCause());
        throw ioe;
      }
    }
  }
}
