use chrono::Local;
use clap::ValueEnum;

use crate::error::RustDFSError;

#[derive(Debug, Clone)]
pub struct OutManager {
    pub verbosity: Verbosity,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, ValueEnum)]
pub enum Verbosity {
    Silent,
    Error,
    Info,
}

impl OutManager {
    pub fn write(&self, verbosity: Verbosity, provider: impl FnOnce() -> String) {
        if self.verbosity == Verbosity::Silent || verbosity > self.verbosity {
            return;
        }

        let msg = provider();
        let ts = Local::now().format("%Y-%m-%d %H:%M:%S");

        match verbosity {
            Verbosity::Error => eprintln!("[{}] [{:?}] {}", ts, verbosity, msg),
            _ => println!("[{}] [{:?}] {}", ts, verbosity, msg),
        }
    }

    /**
     * Logs a [RustDFSError] instance.
     */
    pub fn write_err(&self, err: &RustDFSError) {
        self.write(Verbosity::Error, || err.to_string());
    }
}
