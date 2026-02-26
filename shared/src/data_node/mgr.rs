use std::collections::HashMap;

use tonic::Status;

use crate::base::result::ServiceResult;
use super::conn::DataNodeConn;

#[derive(Debug)]
pub struct DataNodeManager {
    connections: HashMap<String, DataNodeConn>,
}

impl DataNodeManager {

    pub fn new(
        connections: HashMap<String, DataNodeConn>,
    ) -> Self {
        DataNodeManager {
            connections: connections,
        }
    }

    pub fn get_node_ids(
        &self,
    ) -> Vec<&String> {
        self.connections
            .keys()
            .collect()
    }

    pub fn get_conn(
        &self,
        id: &str,
    ) -> ServiceResult<&DataNodeConn> {
        self.connections
            .get(id)
            .ok_or_else(|| status_err_unknown_node(id))
    }
}

fn status_err_unknown_node(
    node_id: &str,
) -> Status {
    let log = format!("Unknown data node (node = {})", node_id).to_string();
    Status::invalid_argument(log)
}
