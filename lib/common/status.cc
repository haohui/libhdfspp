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
#include "libhdfs++/status.h"

#include <cassert>
#include <cstring>

namespace hdfs {

Status::Status(int code, const char *msg) {
  assert(code != kOk);
  const uint32_t len = strlen(msg);
  char *result = new char[len + 5];
  *reinterpret_cast<uint32_t*>(result) = len;
  result[4] = static_cast<char>(code);
  memcpy(result + 5, msg, len);
  state_ = result;
}

std::string Status::ToString() const {
  if (!state_) {
    return "OK";
  } else {
    uint32_t length = *reinterpret_cast<const uint32_t*>(state_);
    return std::string(state_ + 5, length);
  }
}

const char* Status::CopyState(const char* state) {
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  char* result = new char[size + 5];
  memcpy(result, state, size + 5);
  return result;
}

}
