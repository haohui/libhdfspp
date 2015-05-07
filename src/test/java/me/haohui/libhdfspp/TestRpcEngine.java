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
package me.haohui.libhdfspp;

import com.google.protobuf.BlockingService;
import com.google.protobuf.MessageLite;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcServerException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.junit.AfterClass;
import org.junit.Assert;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import me.haohui.libhdfspp.TestRpcServiceProtos.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRpcEngine {
  private static final byte[] CLIENT_ID = "libhdfspp".getBytes(Charsets.UTF_8);
  private final static String ADDRESS = "localhost";
  private final static int PORT = 0;
  private static InetSocketAddress addr;
  private static Configuration conf;
  private static RPC.Server server;

  @ProtocolInfo(protocolName = "testProto", protocolVersion = 1)
  public interface TestRpcService
      extends TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface {
  }

  static class NativeRPCClient implements TestRpcService, Closeable {
    private final NativeRpcEngine engine;

    NativeRPCClient(NativeRpcEngine engine) {
      this.engine = engine;
    }

    private void rpc(String method, MessageLite request,
        MessageLite.Builder response) throws ServiceException {
      try {
        engine.rpc(method.getBytes(Charsets.UTF_8), request, response);
      } catch (IOException e) {
        throw new ServiceException(e);
      }
    }

    @Override
    public EmptyResponseProto ping(
        RpcController controller, EmptyRequestProto request)
        throws ServiceException {
      EmptyResponseProto.Builder b = EmptyResponseProto.newBuilder();
      rpc("ping", request, b);
      return b.build();
    }

    @Override
    public EchoResponseProto echo(
        RpcController controller, EchoRequestProto request)
        throws ServiceException {
      EchoResponseProto.Builder b = EchoResponseProto.newBuilder();
      rpc("echo", request, b);
      return b.build();
    }

    @Override
    public EmptyResponseProto error(
        RpcController controller, EmptyRequestProto request)
        throws ServiceException {
      EmptyResponseProto.Builder b = EmptyResponseProto.newBuilder();
      rpc("error", request, b);
      return b.build();
    }

    @Override
    public EmptyResponseProto error2(
        RpcController controller, EmptyRequestProto request)
        throws ServiceException {
      EmptyResponseProto.Builder b = EmptyResponseProto.newBuilder();
      rpc("error2", request, b);
      return b.build();
    }

    @Override
    public void close() throws IOException {

    }
  }

  public static class PBServerImpl implements TestRpcService {
    @Override
    public EmptyResponseProto ping(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      byte[] clientId = Server.getClientId();
      Assert.assertArrayEquals(CLIENT_ID, clientId);
      return EmptyResponseProto.newBuilder().build();
    }

    @Override
    public EchoResponseProto echo(RpcController unused, EchoRequestProto request)
        throws ServiceException {
      return EchoResponseProto.newBuilder().setMessage(request.getMessage())
          .build();
    }

    @Override
    public EmptyResponseProto error(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      throw new ServiceException("error", new RpcServerException("error"));
    }

    @Override
    public EmptyResponseProto error2(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      throw new ServiceException("error", new RuntimeException("testException"));
    }
  }

  @BeforeClass
  public static void setUp() throws IOException {
    conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH, 1024);
    // Set RPC engine to protobuf RPC engine
    RPC.setProtocolEngine(conf, TestRpcService.class, ProtobufRpcEngine.class);

    // Create server side implementation
    PBServerImpl serverImpl = new PBServerImpl();
    BlockingService service = TestRpcServiceProtos.TestProtobufRpcProto
        .newReflectiveBlockingService(serverImpl);

    // Get RPC server for server side implementation
    server = new RPC.Builder(conf).setProtocol(TestRpcService.class)
        .setInstance(service).setBindAddress(ADDRESS).setPort(PORT).build();
    addr = NetUtils.getConnectAddress(server);
    server.start();
  }


  @AfterClass
  public static void tearDown() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  @Test (timeout=5000)
  public void testProtoBufRpc() throws Exception {
    try (NativeIoService ioService = new NativeIoService();
        NativeRpcEngine engine = new NativeRpcEngine(
        ioService, CLIENT_ID, "testProto", 1);
         IoServiceExecutor executor = new IoServiceExecutor(ioService)
    ) {
      executor.start();
      engine.connect(addr);
      engine.start();
      TestRpcService client = new NativeRPCClient(engine);
      testProtoBufRpc(client);
    }
  }

  // separated test out so that other tests can call it.
  public static void testProtoBufRpc(TestRpcService client) throws Exception {
    // Test ping method
    EmptyRequestProto emptyRequest = EmptyRequestProto.newBuilder().build();
    client.ping(null, emptyRequest);

    // Test echo method
    EchoRequestProto echoRequest = EchoRequestProto.newBuilder()
        .setMessage("hello").build();
    EchoResponseProto echoResponse = client.echo(null, echoRequest);
    Assert.assertEquals(echoResponse.getMessage(), "hello");

    // Test error method - error should be thrown as RemoteException
    try {
      client.error(null, emptyRequest);
      Assert.fail("Expected exception is not thrown");
    } catch (ServiceException ignored) {
    }
  }

  @Test(timeout=5000)
  public void testProtoBufRandomException() throws Exception {
    try (NativeIoService ioService = new NativeIoService();
         NativeRpcEngine engine = new NativeRpcEngine(ioService, CLIENT_ID,
                                                      "testProto", 1);
         IoServiceExecutor executor = new IoServiceExecutor(ioService)
    ) {
      executor.start();
      engine.connect(addr);
      engine.start();
      TestRpcService client = new NativeRPCClient(engine);
      EmptyRequestProto emptyRequest = EmptyRequestProto.newBuilder().build();

      try {
        client.error2(null, emptyRequest);
        Assert.fail("Expected exception is not thrown");
      } catch (ServiceException ignored) {
      }
    }
  }
}
