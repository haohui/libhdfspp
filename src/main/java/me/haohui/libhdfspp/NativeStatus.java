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

import com.google.common.base.Preconditions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class NativeStatus {
  enum Errno {
    OK(0, null),
    GENERIC_ERROR(1, IOException.class),
    INVALID_ENCRYPTION_KEY(2, IOException.class),
    UNIMPLEMENTED(3, NotImplementedException.class),
    EINVAL(22, IllegalArgumentException.class),
    EAGAIN(35, FileNotFoundException.class),
    ECONNRESET(54, ConnectException.class),
    ETIMEDOUT(60, SocketTimeoutException.class),
    EXCEPTION(256, Exception.class);

    private static final Map<Integer, Errno> ERRNO_MAP = Collections
        .unmodifiableMap(initializeMapping());

    private static Map<Integer, Errno> initializeMapping() {
      HashMap<Integer, Errno> statMap = new HashMap<Integer, Errno>();
      for (Errno s : Errno.values()) {
        statMap.put(s.value, s);
      }
      return statMap;
    }

    private final int value;
    Constructor<? extends Exception> constructor;
    private final Class<? extends Exception> exceptionClass;

    private Errno(int value, Class<? extends Exception> exceptionClass) {
      this.value = value;
      this.exceptionClass = exceptionClass;

      if (this.exceptionClass != null)
        try {
          constructor = this.exceptionClass.getConstructor(String.class);
        } catch (NoSuchMethodException | SecurityException e) {
        }
    }

    private IOException newException(String message) {
      if (constructor != null) {
        try {
          return (IOException) constructor.newInstance(message);
        } catch (InstantiationException | IllegalAccessException
            | IllegalArgumentException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      } else {
        return null;
      }
    }
  }

  private final byte[] state;

  NativeStatus(byte[] state) {
    this.state = state;
  }

  boolean ok() {
    return state == null;
  }

  private Errno code() {
    if (ok()) {
      return Errno.OK;
    } else {
      return Errno.ERRNO_MAP.get(ByteBuffer.wrap(state, 0, 8).getInt());
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
    if (ok()) {
      return;
    }

    Errno code = code();
    if (code == null) {
      throw new IOException(message());
    }

    //
    // case: handle exceptions expected to be thrown as RuntimeException,
    // otherwise IOException is assumed.
    //
    switch (code) {
    case UNIMPLEMENTED:
      throw new NotImplementedException(message());
    case EINVAL:
      throw new IllegalArgumentException(message());
    case EXCEPTION:
      throw new IOException(message());
    default:
      throw code.newException(message());
    }
  }
}
