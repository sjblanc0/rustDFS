use clap::Parser;

use super::logging::LogLevel;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct RustDFSArgs {
    #[arg(long)]
    pub host: String,

    #[arg(long)]
    pub port: u16,

    #[arg(short, long, default_value_t = false)]
    pub silent: bool,

    #[arg(short, long, value_enum, default_value_t = LogLevel::Info)]
    pub log_level: LogLevel,
}

impl RustDFSArgs {

    pub fn new() -> Self {
        Self::parse()
    }
}