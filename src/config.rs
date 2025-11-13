mod config {

    use std::fs::File;
    use std::io::Read;
    use std::net::SocketAddr;
    use std::vec::Vec;
    use std::error::Error;
    use serde::Deserialize;
    use toml;

    const CONFIG_FILE_GLOBAL: &str = "/etc/rustdfs/rdfsconf.toml";

    const DEFAULT_PORT: u16 = 1024;
    const DEFAULT_PEER_NODES: Vec<SocketAddr> = Vec::new();
    

    #[derive(Deserialize)]
    #[serde(default)]
    pub struct RustDFSConfig {
        #[serde(rename = "server-config")]
        pub server_config: ServerConfig,
    }


    #[derive(Deserialize)]
    #[serde(default)]
    pub struct ServerConfig {
        #[serde(rename = "port")]
        pub port: u16,
        #[serde(rename = "peer-nodes")]
        pub peer_nodes: Vec<SocketAddr>,
    }


    impl RustDFSConfig {

        pub fn new() -> Self {
            let global = Self::extract_to_config(CONFIG_FILE_GLOBAL);

            return match global {
                Ok(config) => config,
                Err(err) => {
                    println!("Error  config file: {}", err);
                    println!("Providing default configuration.");
                    Self::default()
                }
            };
        }


        fn extract_to_config(path: &str) -> Result<Self, Box<dyn Error>> {
            let contents: String = Self::extract_to_string(path)?;

            return Ok(toml::from_str(&contents)?);
        }


        fn extract_to_string(path: &str) -> Result<String, Box<dyn Error>> {
            let mut contents: String = String::new();

            File::open(path)?.read_to_string(&mut contents)?;

            return Ok(contents);
        }
    }


    impl Default for RustDFSConfig {

        fn default() -> Self {
            return RustDFSConfig {
                server_config: ServerConfig::default(),
            };
        }
    }


    impl Default for ServerConfig {
        
        fn default() -> Self {
            return ServerConfig {
                port: DEFAULT_PORT,
                peer_nodes: DEFAULT_PEER_NODES,
            };
        }
    }
}


fn main() {
    let config = config::RustDFSConfig::new();
    println!("Config port: {}", config.server_config.port);
    if config.server_config.peer_nodes.is_empty() {
        println!("No peer nodes configured.");
    } else {
        for node in &config.server_config.peer_nodes {
            println!("Config peer node: {}:{}", node.ip(), node.port());
        }
    }
}