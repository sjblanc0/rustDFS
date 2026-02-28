use std::collections::HashMap;

use tokio::sync::RwLock;

use rustdfs_shared::base::result::ServiceResult;


type FileMapping = HashMap<String, Vec<BlockDescriptor>>;

#[derive(Debug)]
pub struct NameManager {
    files: RwLock<FileMapping>,
}

#[derive(Debug, Clone)]
pub struct BlockDescriptor {
    pub id: String,
    pub node_ids: Vec<String>,
}

impl NameManager {

    // this is going to have to handle loading persisted
    // name data on init
    pub fn new() -> Self {
        NameManager {
            files: RwLock::new(
                HashMap::new(),
            ),
        }
    }

    pub async fn add_file(
        &self, 
        file_name: String, 
        blocks: Vec<BlockDescriptor>,
    ) {
        let mut files = self.files
            .write()
            .await;

        files.insert(file_name.to_string(), blocks);
    }

    pub async fn get_blocks(
        &self, 
        file_name: &str,
    ) -> ServiceResult<Vec<BlockDescriptor>> {
        let files = self.files
            .read()
            .await;

        Ok(
            files[file_name]
                .clone()
        )
    }
}
