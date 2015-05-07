package me.haohui.libhdfspp;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

class NativeRemoteBlockReader implements Closeable {
  private final long handle;

  NativeRemoteBlockReader(NativeTcpConnection connection) {
    this.handle = create(connection.handle());
  }

  long handle() {
    return handle;
  }

  void connect(byte[] clientName, Token<DelegationTokenIdentifier> token,
      ExtendedBlock block, long length, long offset) throws IOException {
    byte[] tokenBytes = token == null ? null : PBHelper.convert(token)
        .toByteArray();
    byte[] blockBytes = PBHelper.convert(block).toByteArray();
    byte[] stat = connect(handle, clientName, tokenBytes, blockBytes, length,
                          offset);
    NativeStatus status = new NativeStatus(stat);
    status.checkForIOException();
  }

  int read(ByteBuffer dst) throws IOException {
    Preconditions.checkArgument(dst.isDirect());
    byte[][] s = new byte[1][];
    int v = readSome(handle, dst, dst.position(), dst.limit(), s);
    NativeStatus stat = new NativeStatus(s[0]);
    stat.checkForIOException();
    dst.position(dst.position() + v);
    return v;
  }

  @Override
  public void close() throws IOException {
    destroy(handle);
  }

  private native static long create(long connection);
  private native static void destroy(long handle);
  private native static byte[] connect(long handle, byte[] clientName, byte[]
      token, byte[] block, long length, long offset);
  private native static int readSome(
      long handle, ByteBuffer dst, int position, int limit, byte[][] status);
}
