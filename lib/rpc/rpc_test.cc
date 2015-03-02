#include "rpc_engine.h"
#include "ClientNamenodeProtocol.pb.h"

#include <asio.hpp>
#include <iostream>

int main(int argc, char *argv[]) {
  using namespace hdfs;
  using namespace ::hadoop::hdfs;
  using ::asio::ip::tcp;
  
  ::asio::io_service io_service;
  if (argc != 4) {
    std::cerr 
        << "Test whehter a file exists on the HDFS cluster.\n"
        << "Usage: " << argv[0] << " "
        << "<nnhost> <nnport> <file>\n";
    return 1;
  }

  RpcEngine engine(&io_service, "libhdfs++", "org.apache.hadoop.hdfs.protocol.ClientProtocol", 1);
  GetFileInfoRequestProto *req_proto = new GetFileInfoRequestProto();
  req_proto->set_src(argv[3]);
  
  std::shared_ptr<RpcRequestBase> req = std::make_shared<RpcRequestBase>(
      "getFileInfo",
      std::move(std::unique_ptr<::google::protobuf::MessageLite>(req_proto)));

  auto resp = std::make_shared<GetFileInfoResponseProto>();
  RpcConnection *conn = &engine.connection();

  tcp::resolver resolver(io_service);
  tcp::resolver::query query(tcp::v4(), argv[1], argv[2]);
  tcp::resolver::iterator iterator = resolver.resolve(query);

  conn->Connect(*iterator, [conn,req,resp](const Status &status) {
      if (!status.ok()) {
        std::cerr << "Connection failed: "<< status.ToString() << std::endl;
        return;
      }
      conn->Handshake([conn,req,resp](const Status &status) {
          if (!status.ok()) {
            std::cerr << "Handshake failed: "<< status.ToString() << std::endl;
            return;
          }
          conn->AsyncRpc(req, resp, [resp](const Status &status) {
              if (!status.ok()) {
                std::cerr << "Async RPC Failed: "<< status.ToString() << std::endl;
                return;
              }
              std::cerr << "File exists: " << resp->has_fs() << std::endl;
            });
        });
    });
  io_service.run();
  return 0;
}
