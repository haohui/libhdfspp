#include "bindings.h"

#include "me_haohui_libhdfspp_NativeFileSystem.h"
#include "me_haohui_libhdfspp_NativeInputStream.h"
#include "me_haohui_libhdfspp_NativeRemoteBlockReader.h"

#include "libhdfs++/hdfs.h"
#include "reader/block_reader.h"

#include <asio/ip/tcp.hpp>

using namespace ::hdfs;
using ::asio::ip::tcp;

JNIEXPORT jlong JNICALL
Java_me_haohui_libhdfspp_NativeFileSystem_create(JNIEnv *env, jclass,
                                                 jlong io_service_handle,
                                                 jstring jserver,
                                                 jint port,
                                                 jobjectArray jstatus) {
  IoService *io_service = reinterpret_cast<IoService*>(io_service_handle);
  const char *server = env->GetStringUTFChars(jserver, nullptr);
  FileSystem *fsptr;
  Status stat = FileSystem::New(io_service, server, port, &fsptr);
  jlong result = 0;
  if (stat.ok()) {
    result = reinterpret_cast<uintptr_t>(fsptr);
  } else {
    SetStatusArray(env, jstatus, stat);
  }
    
  env->ReleaseStringUTFChars(jserver, server);
  return result;
}

JNIEXPORT jlong JNICALL
Java_me_haohui_libhdfspp_NativeFileSystem_open(JNIEnv *env, jclass,
                                               jlong handle,
                                               jstring jpath,
                                               jobjectArray jstatus) {
  FileSystem *self = reinterpret_cast<FileSystem*>(handle);
  const char *path = env->GetStringUTFChars(jpath, nullptr);
  InputStream *isptr;
  Status stat = self->Open(path, &isptr);
  jlong result = 0;
  if (stat.ok()) {
    result = reinterpret_cast<uintptr_t>(isptr);
  } else {
    SetStatusArray(env, jstatus, stat);
  }
  env->ReleaseStringUTFChars(jpath, path);
  return result;
}

JNIEXPORT void JNICALL
Java_me_haohui_libhdfspp_NativeFileSystem_destroy(JNIEnv *, jclass, jlong handle) {
  FileSystem *self = reinterpret_cast<FileSystem*>(handle);
  delete self;
}

JNIEXPORT void JNICALL
Java_me_haohui_libhdfspp_NativeInputStream_destroy(JNIEnv *, jclass, jlong handle) {
  InputStream *self = reinterpret_cast<InputStream*>(handle);
  delete self;  
}

JNIEXPORT jint JNICALL
Java_me_haohui_libhdfspp_NativeInputStream_positionRead(JNIEnv *env, jclass,
                                                        jlong handle,
                                                        jobject jbuf,
                                                        jint position,
                                                        jint limit,
                                                        jlong offset,
                                                        jobjectArray jstatus) {
  InputStream *self = reinterpret_cast<InputStream*>(handle);
  char *buf = reinterpret_cast<char*>(env->GetDirectBufferAddress(jbuf));
  Status stat;
  if (!buf || position > limit) {
    stat = Status::InvalidArgument("Invalid buffer");
    SetStatusArray(env, jstatus, stat);
    return 0;
  }
  size_t read_bytes = 0;
  stat = self->PositionRead(buf + position, limit - position, offset, &read_bytes);
  if (!stat.ok()) {
    SetStatusArray(env, jstatus, stat);
  }
  return read_bytes;
}

JNIEXPORT jlong JNICALL Java_me_haohui_libhdfspp_NativeRemoteBlockReader_create(JNIEnv *, jclass, jlong jconn) {
  tcp::socket *conn = reinterpret_cast<tcp::socket*>(jconn);
  auto self = new RemoteBlockReader<tcp::socket>(BlockReaderOptions(), conn);
  return reinterpret_cast<uintptr_t>(self);
}

JNIEXPORT void JNICALL Java_me_haohui_libhdfspp_NativeRemoteBlockReader_destroy(JNIEnv *, jclass, jlong handle) {
  delete reinterpret_cast<RemoteBlockReader<tcp::socket>*>(handle);
}

JNIEXPORT jbyteArray JNICALL
Java_me_haohui_libhdfspp_NativeRemoteBlockReader_connect(JNIEnv *env, jclass, jlong handle,
                                                         jbyteArray jclient_name, jbyteArray jtoken,
                                                         jbyteArray jblock, jlong length, jlong offset) {
  auto self = reinterpret_cast<RemoteBlockReader<tcp::socket>*>(handle);
  std::string client_name(ReadByteString(env, jclient_name));
  hadoop::common::TokenProto token;
  if (jtoken) {
    ReadPBMessage(env, jtoken, &token);
  }
  hadoop::hdfs::ExtendedBlockProto block;
  ReadPBMessage(env, jblock, &block);
  Status stat = self->connect(client_name, jtoken ? &token : nullptr, &block, length, offset);
  return ToJavaStatusRep(env, stat);
}

JNIEXPORT jint JNICALL
Java_me_haohui_libhdfspp_NativeRemoteBlockReader_readSome(JNIEnv *env, jclass, jlong handle, jobject jdst,
                                                          jint position, jint limit, jobjectArray jstatus) {
  auto self = reinterpret_cast<RemoteBlockReader<tcp::socket>*>(handle);
  char *start = reinterpret_cast<char*>(env->GetDirectBufferAddress(jdst));
  Status stat;
  if (!start || position > limit) {
    stat = Status::InvalidArgument("Invalid buffer");
    SetStatusArray(env, jstatus, stat);
    return 0;
  }
  size_t transferred = self->read_some(asio::buffer(start + position, limit - position), &stat);
  if (!stat.ok()) {
    SetStatusArray(env, jstatus, stat);
  }
  return transferred;
}

