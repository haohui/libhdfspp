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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.BlockingService;
import com.google.protobuf.MessageLite;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcServerException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.ipc.TestRPC.TestProtocol;
//import org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.*;
import org.apache.hadoop.net.NetUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

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
  private static final NativeIoService ioService = new NativeIoService();
  private static final IoServiceExecutor executor = new IoServiceExecutor
      (ioService);

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
    public EmptyResponseProto slowPing(RpcController controller,
        EmptyRequestProto request) throws ServiceException {
      EmptyResponseProto.Builder b = EmptyResponseProto.newBuilder();
      rpc("slowPing", request, b);
      return b.build();
    }
    
    @Override
    public EmptyResponseProto fastPing(RpcController controller,
        EmptyRequestProto request) throws ServiceException {
      EmptyResponseProto.Builder b = EmptyResponseProto.newBuilder();
      rpc("fastPing", request, b);
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
    int fastPingCounter = 0;
    
    public PBServerImpl() {
    }
    
    @Override
    public EmptyResponseProto ping(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      byte[] clientId = Server.getClientId();
      Assert.assertArrayEquals(CLIENT_ID, clientId);
      return EmptyResponseProto.newBuilder().build();
    }
    
    @Override
    public synchronized EmptyResponseProto slowPing(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
        while (fastPingCounter < 2) {
          try {
            System.out.format("slowPing before wait, fastPingCounter = %d\n", fastPingCounter);
            wait(); // slow response until two fast pings happened
          } catch (InterruptedException ignored) {
          }
        }
        fastPingCounter = -2;
      return EmptyResponseProto.newBuilder().build();
    }
    
    @Override
    public synchronized EmptyResponseProto fastPing(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      fastPingCounter++;
      System.out.format("fastPing before notify, fastPingCounter = %d\n", fastPingCounter);
      notify();
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
    executor.start();
  }


  @AfterClass
  public static void tearDown() throws Exception {
    if (server != null) {
      server.stop();
    }
    executor.close();
    ioService.close();
  }
  
  //
  // A class that does an RPC but does not read its response.
  //
  static class SlowRPC implements Runnable {
    private TestRpcService client;
    private volatile boolean done;
   
    SlowRPC(TestRpcService client) {
      this.client = client;
      done = false;
    }

    boolean isDone() {
      return done;
    }

    @Override
    public void run() {
      try {
        EmptyRequestProto request = EmptyRequestProto.newBuilder().build();
        client.slowPing(null, request);   // this would hang until two fast pings happened
        done = true;
      } catch (Exception e) {
        assertTrue("SlowRPC ping exception " + e, false);
      }
    }
  }

  @Test (timeout=5000)
  public void testProtoBufRpc() throws Exception {
    try (NativeRpcEngine engine = new NativeRpcEngine(
        ioService, CLIENT_ID, "testProto", 1);
    ) {
      engine.connect(addr);
      engine.startReadLoop();
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
    try (NativeRpcEngine engine = new NativeRpcEngine(
        ioService, CLIENT_ID, "testProto", 1);
    ) {
      engine.connect(addr);
      engine.startReadLoop();
      TestRpcService client = new NativeRPCClient(engine);
      EmptyRequestProto emptyRequest = EmptyRequestProto.newBuilder().build();

      try {
        client.error2(null, emptyRequest);
        Assert.fail("Expected exception is not thrown");
      } catch (ServiceException ignored) {
      }
    }
  }

  @Test
  public void testShutdownServer() throws Exception {
    try (NativeRpcEngine engine = new NativeRpcEngine(ioService, CLIENT_ID,
        "testProto", 1);) {
      engine.connect(addr);
      engine.startReadLoop();
      TestRpcService client = new NativeRPCClient(engine);
      
      server.stop();
      
      // Test echo method
      EchoRequestProto echoRequest = EchoRequestProto.newBuilder()
          .setMessage("hello").build();
      EchoResponseProto echoResponse = client.echo(null, echoRequest);
      Assert.assertEquals(echoResponse.getMessage(), "hello");
    }
  }

  @Test(timeout = 90000)
  public void testRPCTimeout() throws Exception {
    try (NativeRpcEngine engine = new NativeRpcEngine(ioService, CLIENT_ID,
        "testProto", 1);) {
      engine.connect(addr);
      engine.startReadLoop();
      TestRpcService client = new NativeRPCClient(engine);
      SlowRPC slowrpc = new SlowRPC(client);

      Thread thread = new Thread(slowrpc, "SlowRPC");
      thread.start(); // send a slow RPC, which won't return until two fast
                      // pings
      assertTrue("Slow RPC should not have finished1.", !slowrpc.isDone());

      EmptyRequestProto request1 = EmptyRequestProto.newBuilder().build();
      client.fastPing(null, request1); // first fast ping

      // verify that the first RPC is still stuck
      assertTrue("Slow RPC should not have finished2.", !slowrpc.isDone());
      System.out.format("{%s}\n", "before second fast ping");
      EmptyRequestProto request2 = EmptyRequestProto.newBuilder().build();
      client.fastPing(null, request2); // second fast ping

      // Now the slow ping should be able to be executed
      while (!slowrpc.isDone()) {
        System.out.println("Waiting for slow RPC to get done.");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
    } catch(ServiceException e) {
      //ignore
    } finally {
      System.out.println("Down slow rpc testing");
    }
  }

  @Test(timeout = 90000)
  public void testRPCInterruptedSimple() throws Exception {
    try (NativeRpcEngine engine = new NativeRpcEngine(ioService, CLIENT_ID,
        "testProto", 1);) {
      engine.connect(addr);
      engine.startReadLoop();
      TestRpcService client = new NativeRPCClient(engine);

      EmptyRequestProto emptyRequest = EmptyRequestProto.newBuilder().build();
      client.ping(null, emptyRequest);

      // Interrupt self, try another call
      Thread.currentThread().interrupt();
      try {
        client.ping(null, emptyRequest);
        fail("Interruption did not cause IPC to fail");
      } catch (Exception e) {
        if (!e.toString().contains("InterruptedException")) {
          throw e;
        }
        // clear interrupt status for future tests
        Thread.interrupted();
      } catch (Throwable e) {
        if (!e.toString().contains("Interruption did not cause IPC to fail")) {
          throw e;
        }
      }
    }
  }
}
