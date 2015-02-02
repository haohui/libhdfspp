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
#ifndef LIB_COMMON_MONAD_MONAD_H_
#define LIB_COMMON_MONAD_MONAD_H_

#include <type_traits>
#include <utility>

namespace hdfs {
namespace monad {

template<class, class> struct Bind;

template<typename... A>
struct Monad {
  typedef Monad<A...> MonadType;
  static const bool is_monad = true;
};

template <class MA, class MB
          , class = typename std::enable_if<MA::is_monad>::type
          , class = typename std::enable_if<MB::is_monad>::type
          >
Bind<MA, MB> operator>>=(MA&& ma, MB&& mb) {
  return Bind<MA, MB>(std::forward<MA>(ma), std::forward<MB>(mb));
}

}
}

#endif
