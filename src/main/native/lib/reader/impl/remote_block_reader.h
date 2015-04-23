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
#ifndef IMPL_REMOTE_BLOCK_READER_H_
#define IMPL_REMOTE_BLOCK_READER_H_

#include "common/protobuf_util.h"
#include "common/datatransfer.h"

#include "common/monad/monad.h"
#include "common/monad/bind.h"
#include "common/monad/asio.h"
#include "common/monad/conditional.h"

#include <asio/buffers_iterator.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>

#include <arpa/inet.h>

#include <future>

namespace hdfs {

hadoop::hdfs::OpReadBlockProto ReadBlockProto(
    const std::string &client_name, bool verify_checksum,
    const hadoop::common::TokenProto *token,
    const hadoop::hdfs::ExtendedBlockProto *block,
    uint64_t length, uint64_t offset);

template<class Stream>
template<class ConnectHandler>
void RemoteBlockReader<Stream>::async_connect(const std::string &client_name,
                                              const hadoop::common::TokenProto *token,
                                              const hadoop::hdfs::ExtendedBlockProto *block,
                                              uint64_t length, uint64_t offset,
                                              const ConnectHandler &handler) {
  // The total number of bytes that we need to transfer from the DN is
  // the amount that the user wants (bytesToRead), plus the padding at
  // the beginning in order to chunk-align. Note that the DN may elect
  // to send more than this amount if the read starts/ends mid-chunk.
  bytes_to_read_ = length;
  hadoop::hdfs::OpReadBlockProto p(ReadBlockProto(
      client_name, options_.verify_checksum, token, block, length, offset));

  struct State {
    std::string request;
    hadoop::hdfs::BlockOpResponseProto response;
  };

  auto s = std::make_shared<State>();
  s->request.insert(s->request.begin(), { 0, kDataTransferVersion, Operation::kReadBlock });
  AppendToDelimitedString(&p, &s->request);

  auto prog = monad::Write(stream_, asio::buffer(s->request))
          >>= ReadPBMessageMonad<Stream, hadoop::hdfs::BlockOpResponseProto, 16384>
              (stream_, &s->response);

  auto m = std::shared_ptr<decltype(prog)>(new decltype(prog)(std::move(prog)));
  m->Run([m,s,this,handler,offset](const Status &status) {
      Status stat = status;
      if (stat.ok()) {
        const auto &resp = s->response;
        if (resp.status() == ::hadoop::hdfs::Status::SUCCESS) {
          if(resp.has_readopchecksuminfo()) {
            const auto& checksum_info = resp.readopchecksuminfo();
            chunk_padding_bytes_ = offset - checksum_info.chunkoffset();
          }
          state_ = kReadPacketHeader;
        } else {
          stat = Status::Error(s->response.message().c_str());
        }
      }
      handler(stat);
    });
}

template<class Stream>
struct RemoteBlockReader<Stream>::ReadPacketHeader : monad::Monad<> {
  ReadPacketHeader(RemoteBlockReader<Stream> *self)
      : self_(self)
  {}
  ReadPacketHeader(ReadPacketHeader &&) = default;
  ReadPacketHeader &operator=(ReadPacketHeader &&) = default;
  template<class Next>
  void Run(const Next& next) {
    self_->packet_data_read_bytes_ = self_->packet_len_ = 0;
    auto handler = [next, this](const asio::error_code &ec, size_t) {
      Status status;
      if (ec) {
        status = Status(ec.value(), ec.message().c_str());
      } else {
        self_->packet_len_ = packet_length();
        self_->header_.Clear();
        bool v = self_->header_.ParseFromArray(&buf_[kHeaderStart], header_length());
        assert(v && "Failed to parse the header");
      }
      next(status);
    };

    asio::async_read(*self_->stream_, asio::buffer(buf_),
                     std::bind(&ReadPacketHeader::CompletionHandler, this,
                               std::placeholders::_1, std::placeholders::_2),
                     handler);
  }

 private:
  static const size_t kMaxHeaderSize = 512;
  static const size_t kPayloadLenOffset = 0;
  static const size_t kPayloadLenSize = sizeof(int);
  static const size_t kHeaderLenOffset = 4;
  static const size_t kHeaderLenSize = sizeof(short);
  static const size_t kHeaderStart = kPayloadLenSize + kHeaderLenSize;

  RemoteBlockReader<Stream> *self_;
  std::array<char, kMaxHeaderSize> buf_;

  size_t packet_length() const
  { return ntohl(*reinterpret_cast<const unsigned*>(&buf_[kPayloadLenOffset])); }

  size_t header_length() const
  { return ntohs(*reinterpret_cast<const short*>(&buf_[kHeaderLenOffset])); }

  size_t CompletionHandler(const asio::error_code &ec, size_t transferred) {
    if (ec) {
      return 0;
    } else if (transferred < kHeaderStart) {
      return kHeaderStart - transferred;
    } else {
      return kHeaderStart + header_length() - transferred;
    }
  }

  ReadPacketHeader(const ReadPacketHeader &) = delete;
  ReadPacketHeader &operator=(const ReadPacketHeader &) = delete;
};

template<class Stream>
struct RemoteBlockReader<Stream>::ReadChecksum : monad::Monad<> {
  ReadChecksum(RemoteBlockReader<Stream> *self)
      : self_(self)
  {}
  ReadChecksum(ReadChecksum &&) = default;
  ReadChecksum &operator=(ReadChecksum &&) = default;

  template<class Next>
  void Run(const Next& next) {
    auto handler = [next,this](const asio::error_code &ec, size_t) {
      Status status;
      if (ec) {
        status = Status(ec.value(), ec.message().c_str());
      } else {
        self_->state_ = self_->chunk_padding_bytes_ ? kReadPadding : kReadData;
      }
      next(status);
    };
    self_->checksum_.resize(self_->packet_len_ - sizeof(int) - self_->header_.datalen());
    asio::async_read(*self_->stream_, asio::buffer(self_->checksum_), handler);
  }

 private:
  RemoteBlockReader<Stream> *self_;
  ReadChecksum(const ReadChecksum &) = delete;
  ReadChecksum &operator=(const ReadChecksum &) = delete;
};

template<class Stream>
struct RemoteBlockReader<Stream>::ReadPadding : monad::Monad<> {
  ReadPadding(RemoteBlockReader<Stream> *self)
      : self_(self)
      , padding_(self->chunk_padding_bytes_)
      , bytes_transferred_(std::make_shared<size_t>(0))
      , prog_(ReadData<asio::mutable_buffers_1>(self, bytes_transferred_, asio::buffer(padding_)))
  {}
  ReadPadding(ReadPadding &&) = default;
  ReadPadding &operator=(ReadPadding &&) = default;

  template<class Next>
  void Run(const Next& next) {
    auto h = [next,this](const Status &status) {
      if (status.ok()) {
        assert(reinterpret_cast<const int&>(*bytes_transferred_) == self_->chunk_padding_bytes_);
        self_->chunk_padding_bytes_ = 0;
        self_->state_ = kReadData;
      }
      next(status);
    };
    prog_.Run(h);
  }

 private:
  RemoteBlockReader<Stream> *self_;
  std::vector<char> padding_;
  std::shared_ptr<size_t> bytes_transferred_;
  RemoteBlockReader::ReadData<asio::mutable_buffers_1> prog_;
  ReadPadding(const ReadPadding &) = delete;
  ReadPadding &operator=(const ReadPadding &) = delete;
};

template<class Stream>
template<class MutableBufferSequence>
struct RemoteBlockReader<Stream>::ReadData : monad::Monad<> {
  ReadData(RemoteBlockReader<Stream> *self,
           std::shared_ptr<size_t> bytes_transferred,
           const MutableBufferSequence &buf)
      : self_(self)
      , bytes_transferred_(bytes_transferred)
      , buf_(buf)
  {}
  ReadData(ReadData &&) = default;
  ReadData &operator=(ReadData &&) = default;

  template<class Next>
  void Run(const Next& next) {
    auto handler = [next, this](const asio::error_code &ec, size_t transferred) {
      Status status;
      if (ec) {
        status = Status(ec.value(), ec.message().c_str());
      }
      *bytes_transferred_ += transferred;
      self_->bytes_to_read_ -= transferred;
      self_->packet_data_read_bytes_ += transferred;
      if (self_->packet_data_read_bytes_ >= self_->header_.datalen()) {
        self_->state_ = kReadPacketHeader;
      }
      next(status);
    };

    auto data_len = self_->header_.datalen() - self_->packet_data_read_bytes_;
    async_read(*self_->stream_, buf_, asio::transfer_exactly(data_len), handler);
  }

 private:
  RemoteBlockReader<Stream> *self_;
  std::shared_ptr<size_t> bytes_transferred_;
  MutableBufferSequence buf_;
  ReadData(const ReadData &) = delete;
  ReadData &operator=(const ReadData &) = delete;
};

template<class Stream>
struct RemoteBlockReader<Stream>::AckRead : monad::Monad<> {
  AckRead(RemoteBlockReader<Stream> *self)
      : self_(self)
  {}

  AckRead(AckRead &&) = default;
  AckRead &operator=(AckRead &&) = default;

  template<class Next>
  void Run(const Next& next) {
    hadoop::hdfs::ClientReadStatusProto p;
    p.set_status(self_->options_.verify_checksum ?
                 hadoop::hdfs::Status::CHECKSUM_OK : hadoop::hdfs::Status::SUCCESS);
    auto req = std::make_shared<std::string>();
    AppendToDelimitedString(&p, req.get());
    auto prog = monad::Write(self_->stream_, asio::buffer(*req));
    auto m = std::shared_ptr<decltype(prog)>(new decltype(prog)(std::move(prog)));
    m->Run([m,req,next,this](const Status &status) {
        if (status.ok()) {
          self_->state_ = RemoteBlockReader<Stream>::kFinished;
        }
        next(status);
      });
  }

 private:
  RemoteBlockReader<Stream> *self_;
  AckRead(const AckRead &) = delete;
  AckRead &operator=(const AckRead &) = delete;
};

template<class Stream>
template<class MutableBufferSequence, class ReadHandler>
void RemoteBlockReader<Stream>::async_read_some(const MutableBufferSequence& buffers,
                                                const ReadHandler &handler) {
  assert(state_ != kOpen && "Not connected");
  auto bytes_transferred = std::make_shared<size_t>();
  *bytes_transferred = 0;

  // TODO: Verify checksum

  auto prog = EnableIf([this]() { return state_ == kReadPacketHeader; },
                       std::move(ReadPacketHeader(this) >>= ReadChecksum(this)))
          >>= EnableIf([this]() { return state_ == kReadPadding && chunk_padding_bytes_; },
                       ReadPadding(this))
          >>= ReadData<MutableBufferSequence>(this, bytes_transferred, buffers)
          >>= EnableIf([this]() { return bytes_to_read_ <= 0; }, AckRead(this));

  auto m = std::shared_ptr<decltype(prog)>(new decltype(prog)(std::move(prog)));
  m->Run([m,this,handler,bytes_transferred](const Status &status) {
      handler(status, *bytes_transferred);
    });
}

template<class Stream>
template<class MutableBufferSequence>
size_t RemoteBlockReader<Stream>::read_some(const MutableBufferSequence& buffers, Status *status) {
  size_t transferred = 0;
  auto done = std::make_shared<std::promise<void> >();
  auto future = done->get_future();
  async_read_some(buffers, [status,&transferred,done](const Status &stat, size_t t) {
      *status = stat;
      transferred = t;
      done->set_value();
    });
  future.wait();
  return transferred;
}

template<class Stream>
Status RemoteBlockReader<Stream>::connect(const std::string &client_name,
                                          const hadoop::common::TokenProto *token,
                                          const hadoop::hdfs::ExtendedBlockProto *block,
                                          uint64_t length, uint64_t offset) {
  auto stat = std::make_shared<std::promise<Status>>();
  std::future<Status> future(stat->get_future());
  async_connect(client_name, token, block, length, offset,
                [stat](const Status &status) { stat->set_value(status); });
  return future.get();
}

}

#endif
