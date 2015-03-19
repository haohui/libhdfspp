#ifndef COMMON_UTIL_H_
#define COMMON_UTIL_H_

#include "libhdfs++/status.h"

#include <openssl/bio.h>
#include <openssl/evp.h>

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

static inline std::string Base64Encode(const std::string &src) {
  int encoded_size = (src.size() + 2) / 3 * 4;
  std::string dst;
  dst.resize(encoded_size);
  BIO *bio = BIO_new_mem_buf(const_cast<char*>(dst.c_str()), dst.size());
  BIO *b64 = BIO_new(BIO_f_base64());
  bio = BIO_push(b64, bio);
  BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
  BIO_write(bio, &src.at(0), src.size());
  BIO_flush(bio);
  BIO_free_all(bio);
  return dst;
}

}

#endif
