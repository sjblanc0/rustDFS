use clap::ValueEnum;
use chrono::Local;
use std::fs::{self, write};
use std::path::Path;

use super::error::RustDFSError;
use super::result::Result;

#[derive(Debug, Clone)]
pub struct LogManager {
    file: String,
    level: LogLevel,
    silent: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, ValueEnum)]
pub enum LogLevel {
    Error,
    Info,
}

impl LogManager {

    pub fn new(
        file: String, 
        level: LogLevel, 
        silent: bool
    ) -> Result<Self> {
        let path = Path::new(&file);
        let parent = path.parent()
            .unwrap_or(Path::new(""));

        if !parent.as_os_str().is_empty() && !parent.is_dir() {
            fs::create_dir_all(parent)
                .map_err(|e| RustDFSError::err_create_log_dir(
                    parent.to_str().unwrap_or(&file),
                    e,
                ))?;
        }

        Ok(
            LogManager {
                file,
                level,
                silent,
            }
        )
    }

    pub fn write(
        &self,
        level: LogLevel, 
        provider: impl FnOnce() -> String,
    ) {
        if level > self.level {
            return;
        }

        let msg = provider();
        let ts = Local::now()
            .format("%Y-%m-%d %H:%M:%S");

        let _ = write(&self.file, format!("[{}] [{:?}] {}", ts, level, msg));

        if !self.silent {
            match level {
                LogLevel::Error => eprintln!("[{}] [{:?}] {}", ts, level, msg),
                _ => println!("[{}] [{:?}] {}", ts, level, msg),
            }
        }
    }
}

