#include "rpc_engine.h"
#include "ClientNamenodeProtocol.pb.h"

#include <asio.hpp>

#include <iostream>
#include <thread>

int main(int argc, char *argv[]) {
  using namespace hdfs;
  using namespace ::hadoop::hdfs;
  using ::asio::ip::tcp;

  ::asio::io_service io_service;
  if (argc != 4) {
    std::cerr << "Test whehter a file exists on the HDFS cluster.\n"
              << "Usage: " << argv[0] << " "
              << "<nnhost> <nnport> <file>\n";
    return 1;
  }

  RpcEngine engine(&io_service, "libhdfs++",
                   "org.apache.hadoop.hdfs.protocol.ClientProtocol", 1);
  GetFileInfoRequestProto req;
  auto resp = std::make_shared<GetFileInfoResponseProto>();
  req.set_src(argv[3]);

  tcp::resolver resolver(io_service);
  tcp::resolver::query query(tcp::v4(), argv[1], argv[2]);
  tcp::resolver::iterator iterator = resolver.resolve(query);
  std::vector<::asio::ip::tcp::endpoint> servers(iterator,
                                                 tcp::resolver::iterator());

  ::asio::io_service::work work(io_service);
  std::thread io_service_thread([&io_service]() { io_service.run(); });

  Status status = engine.Connect(servers);
  if (!status.ok()) {
    std::cerr << "Connection failed: " << status.ToString() << std::endl;
    return -1;
  }
  std::cout << "Connected to " << servers.front() << std::endl;
  engine.Start();
  engine.AsyncRpc("getFileInfo", &req, resp, [resp, &engine, &io_service](
      const Status &status) {
                    if (!status.ok()) {
                      std::cerr << "Async RPC Failed: " << status.ToString() << std::endl;
                      engine.Shutdown();
                      io_service.post([&io_service]() { io_service.stop(); });
                      return;
                    }
                    std::cerr << "File exists: " << resp->has_fs() << std::endl;
                    engine.Shutdown();
                    io_service.post([&io_service]() { io_service.stop(); });
                  });
  io_service_thread.join();
  return 0;
}
