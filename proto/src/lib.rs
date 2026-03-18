/**
 * Generated protobuf/gRPC modules for RustDFS.
 *
 * [name] - Name Node service (file metadata, block allocation, leases).
 * [data] - Data Node service (block read/write, replication).
 */

/// Name Node gRPC service and message types.
pub mod name {
    use tonic::include_proto;
    include_proto!("name_node");
}

/// Data Node gRPC service and message types.
pub mod data {
    use tonic::include_proto;
    include_proto!("data_node");
}
