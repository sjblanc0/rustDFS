use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, ValueEnum)]
pub enum Operation {
    Write,
    Read,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct RustDFSArgs {
    #[arg(value_enum)]
    pub op: Operation,

    pub host: String,

    pub source: String,

    pub dest: String,
}

impl RustDFSArgs {

    pub fn new() -> Self {
        Self::parse()
    }
}
