package me.haohui.libhdfspp;

import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

class NativeFileSystem implements Closeable {
  private final long handle;

  NativeFileSystem(NativeIoService ioService, InetSocketAddress addr)
      throws IOException {
    byte[][] s = new byte[1][];
    this.handle = create(ioService.handle(), addr.getHostString(),
                         addr.getPort(), s);
    NativeStatus stat = new NativeStatus(s[0]);
    stat.checkForIOException();
  }

  NativeInputStream open(Path path) throws IOException {
    byte[][] s = new byte[1][];
    long h = open(handle, path.toString(), s);
    NativeStatus stat = new NativeStatus(s[0]);
    stat.checkForIOException();
    return new NativeInputStream(h);
  }

  @Override
  public void close() throws IOException {
    destroy(handle);
  }

  private native static long create(long ioService, String server, int port,
      byte[][] status);
  private native static long open(long handle, String path, byte[][] status);
  private native static void destroy(long handle);
}
