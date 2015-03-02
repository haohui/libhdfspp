#ifndef COMMON_UTIL_H_
#define COMMON_UTIL_H_

#include "libhdfs++/status.h"

#include <asio/error_code.hpp>

namespace hdfs {

static inline Status ToStatus(const ::asio::error_code &ec) {
  if (ec) {
    return Status(ec.value(), ec.message().c_str());
  } else {
    return Status::OK();
  }
}

}

#endif
