use tonic::{include_file_descriptor_set, include_proto};

include_proto!("data_node");
include_proto!("name_node");
pub const FILE_DESCRIPTOR_SET: &[u8] = include_file_descriptor_set!("node_descriptor");
