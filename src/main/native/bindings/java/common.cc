#include "bindings.h"

#include "me_haohui_libhdfspp_NativeIoService.h"
#include "me_haohui_libhdfspp_NativeTcpConnection.h"
#include "me_haohui_libhdfspp_NativeRpcEngine.h"

#include "common/libhdfs++-internal.h"
#include "common/util.h"
#include "rpc/rpc_engine.h"

#include <asio/ip/tcp.hpp>

#include <cassert>

using namespace ::hdfs;
using ::asio::ip::tcp;

JNIEXPORT jlong JNICALL Java_me_haohui_libhdfspp_NativeIoService_create(JNIEnv *, jclass) {
  return reinterpret_cast<uintptr_t>(IoService::New());
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeIoService_nativeRun(JNIEnv *, jclass, jlong handle) {
  IoService *self = reinterpret_cast<IoService*>(handle);
  self->Run();
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeIoService_stop(JNIEnv *, jclass, jlong handle) {
  IoService *self = reinterpret_cast<IoService*>(handle);
  self->Stop();
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeIoService_destroy(JNIEnv *, jclass, jlong handle) {
  IoService *self = reinterpret_cast<IoService*>(handle);
  delete self;
}

JNIEXPORT jlong JNICALL Java_me_haohui_libhdfspp_NativeTcpConnection_create(JNIEnv *, jclass, jlong io_service_handle) {
  IoServiceImpl *io_service = reinterpret_cast<IoServiceImpl*>(io_service_handle);
  tcp::socket *sock = new tcp::socket(io_service->io_service());
  return reinterpret_cast<uintptr_t>(sock);
}

JNIEXPORT jbyteArray JNICALL Java_me_haohui_libhdfspp_NativeTcpConnection_connect(JNIEnv *env, jclass, jlong handle, jstring jhost, jint port) {
  tcp::socket *self = reinterpret_cast<tcp::socket*>(handle);
  const char *host = env->GetStringUTFChars(jhost, nullptr);
  tcp::endpoint ep(asio::ip::address::from_string(host), port);
  asio::error_code ec;
  self->connect(ep, ec);
  env->ReleaseStringUTFChars(jhost, host);
  if (!ec) {
    return nullptr;
  }
  assert (false && "Unimplemented");
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeTcpConnection_disconnect(JNIEnv *, jclass, jlong handle) {
  tcp::socket *self = reinterpret_cast<tcp::socket*>(handle);
  ::asio::error_code ec;
  self->close(ec);
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeTcpConnection_destroy(JNIEnv *, jclass, jlong handle) {
  tcp::socket *self = reinterpret_cast<tcp::socket*>(handle);
  delete self;
}

JNIEXPORT jlong JNICALL
Java_me_haohui_libhdfspp_NativeRpcEngine_create(JNIEnv *env, jclass, jlong io_service_handle,
                                                jbyteArray jclient_name, jbyteArray jprotocol,
                                                jint version) {
  IoServiceImpl *io_service = reinterpret_cast<IoServiceImpl*>(io_service_handle);

  RpcEngine *engine = new RpcEngine(&io_service->io_service(), ReadByteString(env, jclient_name),
                                    ReadByteString(env, jprotocol).c_str(), version);
  return reinterpret_cast<uintptr_t>(engine);
}

JNIEXPORT jbyteArray JNICALL
Java_me_haohui_libhdfspp_NativeRpcEngine_connect(JNIEnv *env, jclass, jlong handle,
                                                 jstring jhost, jint port) {
  RpcEngine *self = reinterpret_cast<RpcEngine*>(handle);
  const char *host = env->GetStringUTFChars(jhost, nullptr);
  tcp::endpoint ep(asio::ip::address::from_string(host), port);
  Status status = self->Connect(ep);
  env->ReleaseStringUTFChars(jhost, host);
  return ToJavaStatusRep(env, status);
}

JNIEXPORT void JNICALL
Java_me_haohui_libhdfspp_NativeRpcEngine_startReadLoop(JNIEnv *, jclass, jlong handle) {
  RpcEngine *self = reinterpret_cast<RpcEngine*>(handle);
  self->StartReadLoop();
}

JNIEXPORT jbyteArray JNICALL
Java_me_haohui_libhdfspp_NativeRpcEngine_rpc(JNIEnv *env, jclass, jlong handle, jbyteArray jmethod,
                                             jbyteArray request, jobjectArray jstatus) {
  RpcEngine *self = reinterpret_cast<RpcEngine*>(handle);
  auto response = std::make_shared<std::string>();
  Status stat = self->RawRpc(ReadByteString(env, jmethod),
                             std::move(ReadByteString(env, request)), response);
  if (!stat.ok()) {
    SetStatusArray(env, jstatus, stat);
    return nullptr;
  }
  jbyteArray jresp = env->NewByteArray(response->size());
  void *b = env->GetPrimitiveArrayCritical(jresp, nullptr);
  memcpy(b, response->c_str(), response->size());
  env->ReleasePrimitiveArrayCritical(jresp, b, 0);
  return jresp;
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeRpcEngine_destroy(JNIEnv *, jclass, jlong handle) {
  delete reinterpret_cast<RpcEngine*>(handle);
}
