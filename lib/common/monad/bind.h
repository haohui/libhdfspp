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
#ifndef LIB_COMMON_MONAD_BIND_H_
#define LIB_COMMON_MONAD_BIND_H_

namespace hdfs {
namespace monad {

template<class First, class Second>
struct Bind : Monad<> {
  Bind(First &&first, Second &&second)
      : first_(std::move(first))
      , second_(std::move(second))
  {}
  
  template<class Next>
  void Run(const Next& next) {
    auto handler = [next,this](const Status &status) {
      if (!status.ok()) {
        next(status);
      } else {
        second_.Run(next);
      }
    };
    first_.Run(handler);
  }

 private:
  First first_;
  Second second_;
};

}
}

#endif
