#include "util.h"

#include <array>
#include <functional>
#include <algorithm>

namespace hdfs {

std::string Base64Encode(const std::string &src) {
  static const char kDictionary[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz"
      "0123456789+/";

  int encoded_size = (src.size() + 2) / 3 * 4;
  std::string dst;
  dst.reserve(encoded_size);

  size_t i = 0;
  while (i + 3 < src.length()) {
    const char *s = &src[i];
    const int r[4] = {
      s[0] >> 2,
      ((s[0] << 4) | (s[1] >> 4)) & 0x3f,
      ((s[1] << 2) | (s[2] >> 6)) & 0x3f,
      s[2] & 0x3f };

    std::transform(r, r + sizeof(r) / sizeof(int), std::back_inserter(dst),
                   [&r](unsigned char v) { return kDictionary[v]; });
    i += 3;
  }

  size_t remained = src.length() - i;
  const char *s = &src[i];

  switch (remained) {
    case 0:
      break;
    case 1: {
      char padding[4] = {
        kDictionary[s[0] >> 2],
        kDictionary[(s[0] << 4) & 0x3f],
        '=', '=' };
      dst.append(padding, sizeof(padding));
    }
      break;
    case 2: {
      char padding[4] = {
        kDictionary[src[i] >> 2],
        kDictionary[((s[0] << 4) | (s[1] >> 4)) & 0x3f],
        kDictionary[(s[1] << 2) & 0x3f],
        '=' };
      dst.append(padding, sizeof(padding));
    }
      break;
    default:
      assert("Unreachable");
      break;
  }
  return dst;
}

}
