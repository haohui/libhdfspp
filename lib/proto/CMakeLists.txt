protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS datatransfer.proto hdfs.proto Security.proto)
add_library(proto ${PROTO_SRCS} ${PROTO_HDRS})
