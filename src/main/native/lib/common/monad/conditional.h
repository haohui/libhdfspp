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
#ifndef LIB_COMMON_MONAD_CONDITIONAL_H_
#define LIB_COMMON_MONAD_CONDITIONAL_H_

namespace hdfs {
namespace monad {

template<class Cond, class M>
struct ConditionalMonad : Monad<> {
  ConditionalMonad(const Cond &cond, M &&monad)
      : cond_(cond)
      , monad_(std::move(monad))
  {}

  ConditionalMonad(ConditionalMonad &&) = default;
  ConditionalMonad& operator=(ConditionalMonad&&) = default;

  template<class Next>
  void Run(const Next& next) {
    if (cond_()) {
      monad_.Run(next);
    } else {
      next(Status());
    }
  }

 private:
  Cond cond_;
  M monad_;
  ConditionalMonad(const ConditionalMonad&) = delete;
  ConditionalMonad& operator=(const ConditionalMonad&) = delete;
};

template<class Cond, class M>
ConditionalMonad<Cond, M> EnableIf(const Cond &cond, M &&monad) {
  return ConditionalMonad<Cond, M>(cond, std::forward<M>(monad));
}

}
}

#endif
