#ifndef COMMON_UTIL_H_
#define COMMON_UTIL_H_

#include "libhdfs++/status.h"

#include <asio/error_code.hpp>

namespace hdfs {

class StatusHelper {
 public:
  static std::pair<const char *, size_t> Rep(const Status &status) {
    const char *state = status.state_;
    size_t length = *reinterpret_cast<const uint32_t*>(state);
    return std::make_pair(state, length + 8);
  }
};

static inline Status ToStatus(const ::asio::error_code &ec) {
  if (ec) {
    return Status(ec.value(), ec.message().c_str());
  } else {
    return Status::OK();
  }
}

}

#endif
