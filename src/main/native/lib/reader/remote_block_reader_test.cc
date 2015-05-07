#include "block_reader.h"

#include <asio.hpp>

#include <iostream>
#include <string>

int main(int argc, char *argv[]) {
  using namespace hdfs;
  using ::asio::ip::tcp;

  if (argc != 8)
  {
    std::cerr
        << "A simple client to read a block in the HDFS cluster.\n"
        << "Usage: " << argv[0] << " "
        << "<poolid> <blockid> <genstamp> <size> <offset> <dnhost> <dnport>\n";
    return 1;
  }

  asio::io_service io_service;

  hadoop::hdfs::ExtendedBlockProto block;
  block.set_poolid(argv[1]);
  block.set_blockid(std::stol(argv[2]));
  block.set_generationstamp(std::stol(argv[3]));
  size_t size = std::stol(argv[4]);
  size_t offset = std::stol(argv[5]);

  tcp::resolver resolver(io_service);
  tcp::resolver::query query(tcp::v4(), argv[6], argv[7]);
  tcp::resolver::iterator iterator = resolver.resolve(query);

  std::shared_ptr<tcp::socket> s(new tcp::socket(io_service));
  asio::connect(*s.get(), iterator);

  BlockReaderOptions options;
  auto reader = std::make_shared<RemoteBlockReader<tcp::socket> >(options, s.get());
  std::unique_ptr<char[]> buf(new char[size]);
  reader->async_connect("libhdfs++", nullptr, &block, size, offset, [&buf,reader,size](const Status &status) {
      if (!status.ok()) {
        std::cerr << "Error:" << status.code() << " " << status.ToString() << std::endl;
      } else {
        reader->async_read_some(asio::buffer(buf.get(), size), [&buf,size](const Status &status, size_t transferred) {
            buf[std::min(transferred, size - 1)] = 0;
            std::cerr << "Done:" << status.code()
                      << " transferred = " << transferred << "\n"
                      << buf.get() << std::endl;
          });
      }
    });
  io_service.run();
  return 0;
}
