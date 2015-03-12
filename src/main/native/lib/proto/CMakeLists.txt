protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS
    datatransfer.proto hdfs.proto Security.proto
    IpcConnectionContext.proto ProtobufRpcEngine.proto RpcHeader.proto
    acl.proto xattr.proto encryption.proto inotify.proto
    ClientNamenodeProtocol.proto)
add_library(proto ${PROTO_SRCS} ${PROTO_HDRS})
