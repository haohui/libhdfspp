#ifndef COMMON_PROTOBUF_UTIL_H_
#define COMMON_PROTOBUF_UTIL_H_

#include "libhdfs++/status.h"
#include "monad/monad.h"
#include "monad/write.h"

#include <google/protobuf/message_lite.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <asio/read.hpp>

#include <functional>

#include <cassert>

namespace hdfs {

static inline void AppendToDelimitedString(const google::protobuf::MessageLite *msg, std::string *data) {
  namespace pbio = google::protobuf::io;
  int size = msg->ByteSize();
  data->reserve(data->size() + pbio::CodedOutputStream::VarintSize32(size) + size);
  pbio::StringOutputStream ss(data);
  pbio::CodedOutputStream os(&ss);
  os.WriteVarint32(size);
  msg->SerializeToCodedStream(&os);
}

template <class Stream, class Message, size_t MaxMessageSize = 512>
struct ReadPBMessageMonad : monad::Monad<> {
  ReadPBMessageMonad(Stream *stream, Message *msg)
      : stream_(stream)
      , msg_(msg)
  {}

  ReadPBMessageMonad(ReadPBMessageMonad &&) = default;
  ReadPBMessageMonad &operator=(ReadPBMessageMonad &&) = default;

  template<class Next>
  void Run(const Next& next) {
    namespace pbio = google::protobuf::io;
    auto handler = [next, this](const asio::error_code &ec, size_t) {
      Status status;
      if (ec) {
        status = Status(ec.value(), ec.message().c_str());
      } else {
        pbio::ArrayInputStream as(&buf_[0], buf_.size());
        pbio::CodedInputStream is(&as);
        uint32_t size = 0;
        bool v = is.ReadVarint32(&size);
        assert(v);
        is.PushLimit(size);
        msg_->Clear();
        v = msg_->MergeFromCodedStream(&is);
        assert(v);
      }
      next(status);
    };
    asio::async_read(*stream_, asio::buffer(buf_),
                     std::bind(&ReadPBMessageMonad::CompletionHandler, this,
                               std::placeholders::_1, std::placeholders::_2),
                     handler);
  }

 private:
  size_t CompletionHandler(const asio::error_code &ec, size_t transferred) {
    if (ec) {
      return 0;
    }

    size_t offset = 0, len = 0;
    for (size_t i = 0; i + 1 < transferred && i < sizeof(int); ++i) {
      len = (len << 7) | (buf_[i] & 0x7f);
      if ((uint8_t)buf_.at(i) < 0x80) {
        offset = i + 1;
        break;
      }
    }

    assert (offset + len < buf_.size());
    return offset ? len + offset - transferred : 1;
  }

  ReadPBMessageMonad(const ReadPBMessageMonad &) = delete;
  ReadPBMessageMonad &operator=(const ReadPBMessageMonad &) = delete;
  Stream *stream_;
  Message *msg_;
  std::array<char, MaxMessageSize> buf_;
};

template <class Stream>
struct WriteDelimitedPBMessageMonad : monad::Monad<> {
  WriteDelimitedPBMessageMonad(Stream *stream,
                               const google::protobuf::MessageLite *msg)
      : stream_(stream)
      , msg_(msg)
  {}

  WriteDelimitedPBMessageMonad(WriteDelimitedPBMessageMonad &&) = default;
  WriteDelimitedPBMessageMonad &operator=(WriteDelimitedPBMessageMonad &&) = default;

  template<class Next>
  void Run(const Next& next) {
    AppendToDelimitedString(msg_, &buf_);
    monad::Write(stream_, asio::buffer(buf_)).Run(next);
  }

 private:
  WriteDelimitedPBMessageMonad(const WriteDelimitedPBMessageMonad &) = delete;
  WriteDelimitedPBMessageMonad &operator=(const WriteDelimitedPBMessageMonad &) = delete;
  Stream *stream_;
  const google::protobuf::MessageLite * msg_;
  std::string buf_;
};

template<class Stream, class Message, size_t MaxMessageSize = 512>
ReadPBMessageMonad<Stream, Message, MaxMessageSize>
ReadPBMessage(Stream *stream, Message *msg) {
  return ReadPBMessageMonad<Stream, Message, MaxMessageSize>(stream, msg);
}

template<class Stream>
WriteDelimitedPBMessageMonad<Stream>
WriteDelimitedPBMessage(Stream *stream, google::protobuf::MessageLite *msg) {
  return WriteDelimitedPBMessageMonad<Stream>(stream, msg);
}

}

#endif
