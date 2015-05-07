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

#include "filesystem.h"

#include "common/util.h"

#include <asio/ip/tcp.hpp>

#include <limits>

namespace hdfs {

static const char kNamenodeProtocol[] = "org.apache.hadoop.hdfs.protocol.ClientProtocol";
static const int kNamenodeProtocolVersion = 1;

using ::asio::ip::tcp;

FileSystem::~FileSystem()
{}

Status FileSystem::New(IoService *io_service, const char *server,
                       unsigned short port, FileSystem **fsptr) {
  std::unique_ptr<FileSystemImpl> impl(new FileSystemImpl(io_service));
  Status stat = impl->Connect(server, port);
  if (stat.ok()) {
    *fsptr = impl.release();
  }
  return stat;
}

FileSystemImpl::FileSystemImpl(IoService *io_service)
    : io_service_(static_cast<IoServiceImpl*>(io_service))
    , engine_(&io_service_->io_service(), RpcEngine::GetRandomClientName(),
              kNamenodeProtocol, kNamenodeProtocolVersion)
    , namenode_(&engine_)
{}

Status FileSystemImpl::Connect(const char *server, unsigned short port) {
  asio::error_code ec;
  tcp::resolver resolver(io_service_->io_service());
  tcp::resolver::query query(tcp::v4(), server, std::to_string(port));
  tcp::resolver::iterator iterator = resolver.resolve(query, ec);

  if (ec) {
    return ToStatus(ec);
  }

  std::vector<tcp::endpoint> servers(iterator, tcp::resolver::iterator());
  Status stat = engine_.Connect(servers);
  if (!stat.ok()) {
    return stat;
  }
  engine_.Start();
  return stat;
}

Status FileSystemImpl::Open(const char *path, InputStream **isptr) {
  using ::hadoop::hdfs::GetBlockLocationsRequestProto;
  using ::hadoop::hdfs::GetBlockLocationsResponseProto;

  GetBlockLocationsRequestProto req;
  auto resp = std::make_shared<GetBlockLocationsResponseProto>();
  req.set_src(path);
  req.set_offset(0);
  req.set_length(std::numeric_limits<long long>::max());
  Status stat = namenode_.GetBlockLocations(&req, resp);
  if (!stat.ok()) {
    return stat;
  }

  *isptr = new InputStreamImpl(this, &resp->locations());
  return Status::OK();
}

}
