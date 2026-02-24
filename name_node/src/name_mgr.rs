use std::collections::HashMap;

use tokio::sync::RwLock;

#[derive(Debug)]
pub struct NameManager {
    files: RwLock<HashMap<String, Vec<String>>>,
    blocks: RwLock<HashMap<String, Vec<String>>>,
}

impl NameManager {

    // this is going to have to handle loading persisted
    // name data on init
    pub fn new() -> Self {
        NameManager {
            files: RwLock::new(HashMap::new()),
            blocks: RwLock::new(HashMap::new()),
        }
    }

    pub async fn add_file(
        &self, 
        file_name: &str, 
        blocks: Vec<String>,
        nodes: Vec<String>,
    ) {
        for id in blocks.iter() {
            self.blocks
                .write()
                .await
                .insert(id.clone(), nodes.clone());
        }

        self.files
            .write()
            .await
            .insert(file_name.to_string(), blocks);
    }
}
