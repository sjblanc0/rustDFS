use tonic::{include_proto, include_file_descriptor_set};

include_proto!("data_node");
pub const DATA_FILE_DESCRIPTOR_SET: &[u8] = include_file_descriptor_set!("data_node_descriptor");
