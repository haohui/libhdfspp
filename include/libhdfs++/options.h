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
#ifndef LIBHDFSPP_OPTIONS_H_
#define LIBHDFSPP_OPTIONS_H_

#include <string>

namespace hdfs {

struct CacheStrategy {
  bool drop_behind_specified;
  bool drop_behind;
  bool read_ahead_specified;
  unsigned long long read_ahead;
  CacheStrategy()
      : drop_behind_specified(false)
      , drop_behind(false)
      , read_ahead_specified(false)
      , read_ahead(false)
  {}
};

enum DropBehindStrategy {
  kUnspecified = 0,
  kEnableDropBehind  = 1,
  kDisableDropBehind = 2,
};

enum EncryptionScheme {
  kNone = 0,
  kAESCTRNoPadding = 1,
};

struct BlockReaderOptions {
  bool verify_checksum;
  CacheStrategy cache_strategy;
  EncryptionScheme encryption_scheme;

  BlockReaderOptions()
      : verify_checksum(true)
      , encryption_scheme(EncryptionScheme::kNone)
  {}
};

}

#endif
