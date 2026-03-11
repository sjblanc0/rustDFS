use chrono::Local;
use clap::ValueEnum;
use mpsc::Sender;
use std::fs::{self, write};
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use tonic::Status;

use super::error::RustDFSError;
use super::result::Result;

/**
 * Manages logging operations for RustDFS.
 * Responsible for writing log messages to a specified log file with different log levels.
 * Also supports console output.
 *
 *  @field channel - Channel for sending log messages to log thread.
 *  @field level - Minimum log level to record.
 *  @field silent - If true, suppresses console output.
 */
#[derive(Debug, Clone)]
pub struct LogManager {
    channel: Sender<String>,
    level: LogLevel,
    silent: bool,
}

/**
 * Log levels for RustDFS logging.
 *
 *  @variant Error - Logs only error messages.
 *  @variant Info - Logs informational messages and errors.
 *  @variant Debug - Logs debug messages, informational messages, and errors.
 */
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, ValueEnum)]
pub enum LogLevel {
    Error,
    Info,
    Debug,
}

impl LogManager {
    /**
     * Creates a new LogManager instance. Ensures the log file's parent
     * directory exists. Spawns dedicated thread to handle logfile writing.
     *  
     * It's important that only one instance is created to avoid write
     * conflicts / spawning unnecessary threads. Instead rely on
     * cloning a shared instance.
     *
     *  @param fp - Path to the log file.
     *  @param level - Minimum log level to record.
     *  @param silent - If true, suppresses console output.
     *  @return Result<LogManager> - Initialized LogManager instance or error.
     */
    pub fn new(fp: String, level: LogLevel, silent: bool) -> Result<Self> {
        let (tx, rx) = mpsc::channel::<String>();
        let path = Path::new(&fp);
        let parent = path.parent().unwrap_or(Path::new(""));

        if !parent.as_os_str().is_empty() && !parent.is_dir() {
            fs::create_dir_all(parent).map_err(RustDFSError::IoError)?;
        }

        thread::spawn(move || {
            loop {
                match rx.recv() {
                    Ok(msg) => {
                        let _ = write(&fp, msg);
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(LogManager {
            channel: tx,
            level,
            silent,
        })
    }

    /**
     * Writes log message to logfile and optionally to console.
     *
     *  @param level - Log level of the message.
     *  @param provider - Closure that provides the log message string.
     */
    pub fn write(&self, level: LogLevel, provider: impl FnOnce() -> String) {
        if level > self.level {
            return;
        }

        let msg = provider();
        let ts = Local::now().format("%Y-%m-%d %H:%M:%S");
        let fmt = format!("[{}] [{:?}] {}\n", ts, level, msg);

        self.channel.send(fmt).ok();

        if !self.silent {
            match level {
                LogLevel::Error => eprintln!("[{}] [{:?}] {}", ts, level, msg),
                _ => println!("[{}] [{:?}] {}", ts, level, msg),
            }
        }
    }

    /**
     * Logs a [RustDFSError] instance.
     */
    pub fn write_err(&self, err: &RustDFSError) {
        self.write(LogLevel::Error, || err.to_string());
    }

    /**
     * Logs a tonic [Status] instance.
     */
    pub fn write_status(&self, status: &Status) {
        self.write(LogLevel::Error, || status.message().to_string());
    }
}
