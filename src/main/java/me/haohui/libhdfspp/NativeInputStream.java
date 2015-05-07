package me.haohui.libhdfspp;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

class NativeInputStream implements Closeable {
  private final long handle;
  NativeInputStream(long handle) {
    this.handle = handle;
  }


  @Override
  public void close() throws IOException {
    destroy(handle);
  }

  int positionRead(ByteBuffer buf, long offset) throws IOException {
    Preconditions.checkArgument(buf.isDirect());
    byte[][] s = new byte[1][];
    int v = positionRead(handle, buf, buf.position(), buf.limit(), offset, s);
    NativeStatus stat = new NativeStatus(s[0]);
    stat.checkForIOException();
    buf.position(buf.position() + v);
    return v;
  }

  private native static void destroy(long handle);
  private native static int positionRead(long handle, ByteBuffer buf,
      int position, int limit, long offset, byte[][] status);
}
