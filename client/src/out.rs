use chrono::Local;
use clap::ValueEnum;

use crate::error::RustDFSError;

/**
 * Manages console output for the client with verbosity filtering.
 *
 *  @field verbosity - Minimum [Verbosity] level to display.
 */
#[derive(Debug, Clone)]
pub struct OutManager {
    pub verbosity: Verbosity,
}

/**
 * Client output verbosity levels.
 *
 *  @variant Silent - No output.
 *  @variant Error - Only errors.
 *  @variant Info - Informational messages and errors.
 */
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, ValueEnum)]
pub enum Verbosity {
    Silent,
    Error,
    Info,
}

impl OutManager {
    /**
     * Writes a message to the console if verbosity is met.
     *
     *  @param verbosity - Level of this message.
     *  @param provider - Closure producing the message string.
     */
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
