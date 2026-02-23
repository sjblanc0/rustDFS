use tonic::{include_proto, include_file_descriptor_set};

include_proto!("name_node");
pub(crate) const NAME_NODE_FILE_DESCRIPTOR_SET: &[u8] = include_file_descriptor_set!("name_node_descriptor");
