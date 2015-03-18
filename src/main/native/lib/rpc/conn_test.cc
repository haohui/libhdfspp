#include "rpc_engine.h"

#include <asio.hpp>
#include <iostream>
#include "test_rpc_service.pb.h"
#include <sstream>

int main(int argc, char *argv[]) {
  using namespace hdfs;
  using ::asio::ip::tcp;

  ::asio::io_service io_service;
  if (argc != 5) {
    std::cerr << "Test simple echoing.\n" << "Usage: " << argv[0] << " "
        << "<host> <port> <message> <iter_num>\n";
    return 1;
  }

  //RpcEngine engine(&io_service, "libhdfs++", "org.apache.hadoop.hdfs.protocol.ClientProtocol", 1);
  //RpcEngine engine(&io_service, "testProto", "org.apache.hadoop.hdfs.protocol.ClientProtocol", 1);
  RpcEngine engine(&io_service, "libhdfs++", "testProto", 1);

  auto resp = std::make_shared<EchoResponseProto>();

  RpcConnection *conn = &engine.connection();

  tcp::resolver resolver(io_service);
  tcp::resolver::query query(tcp::v4(), argv[1], argv[2]);
  tcp::resolver::iterator iterator = resolver.resolve(query);

  const char *echoMsg = argv[3];
  int count = atoi(argv[4]);

  conn->Connect(*iterator, [echoMsg, count, conn ,resp,&io_service](const Status &status) {
      if (!status.ok()) {
        std::cerr << "Connection failed: "<< status.ToString() << std::endl;
        return;
      }
      conn->Handshake([echoMsg, count, conn,resp,&io_service](const Status &status) {
          if (!status.ok()) {
            std::cerr << "Handshake failed: "<< status.ToString() << std::endl;
            return;
          }
          conn->StartReadLoop();
          for (int i = 0; i < count; i++) {
            std::cout << "count: " << i << std::endl;
            std::stringstream ss;
            ss << echoMsg; ss << "-"; ss << i;
            EchoRequestProto req;
            req.set_message(ss.str());

            getchar();

            conn->AsyncRpc("echo", &req, resp, [resp,&io_service](const Status &status) {
                if (!status.ok()) {
                  std::cerr << "Async RPC Failed: "<< status.ToString() << std::endl;
                  io_service.post([&io_service](){ io_service.stop(); });
                  return;
                }
                std::cerr << "echoing: " << resp->message() << std::endl;
                io_service.post([&io_service](){ io_service.stop(); });
              });
          }
        });
    });
  io_service.run();
  return 0;
}
