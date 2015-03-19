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
#ifndef FS_NAMENODE_PROTOCOL_H_
#define FS_NAMENODE_PROTOCOL_H_

#include "ClientNamenodeProtocol.pb.h"
#include "rpc/rpc_engine.h"

namespace hdfs {

class ClientNamenodeProtocol {
 public:
  ClientNamenodeProtocol(RpcEngine *engine)
      : engine_(engine)
  {}

  Status GetBlockLocations(const ::hadoop::hdfs::GetBlockLocationsRequestProto *request,
                           std::shared_ptr<::hadoop::hdfs::GetBlockLocationsResponseProto> response) {
    return engine_->Rpc("getBlockLocations", request, response);
  }
 private:
  RpcEngine *engine_;
};

};

#endif
