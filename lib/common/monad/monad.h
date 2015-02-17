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

/**
 * Monad is the basic abstraction to describe a sequence of
 * asynchronous operations in libhdfs++.
 *
 * The concept of monad is widely adopted in the functional
 * programming community. In the context of pure functional
 * programming, a monad `M` describes the operation as a function
 * `M_f` that "transforms" the program state `E`, that is, `M_f : E ->
 * E`. Therefore, a sequence of operations `o_1, o_2, ...` can be
 * expressed as applying the corresponding sequence of monad functions
 * `m_1, m_2, ...` to the program state:
 *
 *     o_1; o_2; ... <=> ...m_2(m_1(E))...
 *
 * The key benefit of the monadic structures is that monadic functions
 * can be executed *asynchronously* in an event loop while you can
 * still write the program in a synchronous, blocking style. The
 * monadic structure (particularly the \link Bind \endlink operator)
 * encapsulates the details of running functions in the event loops,
 * etc.
 *
 * For more information about monad you can visit:
 *
 *   * https://github.com/inetic/masio
 *   * http://bartoszmilewski.com/2011/07/11/monads-in-c
 *
 * In libhdfs++, one gotcha is that you need to be careful on managing
 * the life cycle of the monads as they can be referneced in the event
 * loop. An simple approach is to wrap the monads using
 * `std::shared_ptr`.
 **/
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
