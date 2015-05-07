package me.haohui.libhdfspp;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestInputStream {
  private static final int MIN_BLOCK_SIZE = 1024;
  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void setUp() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, MIN_BLOCK_SIZE);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
  }

  @AfterClass
  public static void tearDown() throws InterruptedException, IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testReadMultiplePackets() throws IOException, InterruptedException {
    final int PACKET_SIZE = 65536;
    final int FILE_SIZE = 4 * PACKET_SIZE;
    final byte[] contents = new byte[FILE_SIZE];
    final int OFFSET_IN_TRUNK = 1;

    Random rand = new Random();
    rand.nextBytes(contents);
    try (OutputStream os = cluster.getFileSystem().create(new Path("/foo"))) {
      os.write(contents);
    }
    try (NativeIoService ioService = new NativeIoService();
         IoServiceExecutor executor = new IoServiceExecutor(ioService)) {
      executor.start();
      NativeFileSystem fs = null;
      try {
        fs = new NativeFileSystem(ioService, cluster
            .getNameNode().getNameNodeAddress());
        ByteBuffer buf = ByteBuffer.allocateDirect(FILE_SIZE);
        try (NativeInputStream is = fs.open(new Path("/foo"))) {
          int r = is.positionRead(buf, 0);
          assertEquals(FILE_SIZE, r);
          buf.flip();
          assertEquals(FILE_SIZE, buf.remaining());
          byte[] readContents = new byte[buf.remaining()];
          buf.get(readContents);
          Assert.assertArrayEquals(contents, readContents);
        }

        // Read in the middle of the chunk
        buf = ByteBuffer.allocateDirect(FILE_SIZE - OFFSET_IN_TRUNK);
        try (NativeInputStream is = fs.open(new Path("/foo"))) {
          int r = is.positionRead(buf, OFFSET_IN_TRUNK);
          assertEquals(FILE_SIZE - OFFSET_IN_TRUNK, r);
          buf.flip();
          assertEquals(FILE_SIZE - OFFSET_IN_TRUNK, buf.remaining());
          byte[] readContents = new byte[buf.remaining()];
          buf.get(readContents);
          byte[] cont = new byte[FILE_SIZE - OFFSET_IN_TRUNK];
          System.arraycopy(contents, OFFSET_IN_TRUNK, cont, 0, cont.length);
          Assert.assertArrayEquals(cont, readContents);
        }
      } finally {
        executor.close();
        if (fs != null) {
          fs.close();
        }
      }
    }
  }

  @Test
  public void testReadMultipleBlocks() throws IOException, InterruptedException {
    final int BLOCK_SIZE = 128 * 1024;
    final int FILE_SIZE = 2 * BLOCK_SIZE;
    final byte[] contents = new byte[FILE_SIZE];
    final Path path = new Path("/multi-block");
    Random rand = new Random();
    rand.nextBytes(contents);

    try (OutputStream os = cluster.getFileSystem()
        .create(path, true, 8192, (short) 1, BLOCK_SIZE)) {
      os.write(contents);
    }
    try (NativeIoService ioService = new NativeIoService();
         IoServiceExecutor executor = new IoServiceExecutor(ioService)) {
      executor.start();
      try (NativeFileSystem fs = new NativeFileSystem(ioService, cluster
          .getNameNode().getNameNodeAddress())) {
        ByteBuffer buf = ByteBuffer.allocateDirect(FILE_SIZE);
        try (NativeInputStream is = fs.open(path)) {
          long pos = 0;
          while (pos < FILE_SIZE) {
            int r = is.positionRead(buf, pos);
            Assert.assertTrue(r >= 0 && r <= FILE_SIZE);
            pos += r;
          }

          assertEquals(FILE_SIZE, pos);
          buf.flip();
          assertEquals(FILE_SIZE, buf.remaining());
          byte[] readContents = new byte[buf.remaining()];
          buf.get(readContents);
          Assert.assertArrayEquals(contents, readContents);
        }
      }
    }
  }

}
