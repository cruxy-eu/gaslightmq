//! The signals that stop a process: Ctrl-C, and the stop of a container or of a service manager.

use crate::error::ProxyError;

/// The signals that stop a process, which it listens for from the moment it starts.
///
/// On Unix these are `SIGINT`, which Ctrl-C sends, and `SIGTERM`, which `docker stop` and a service manager send.
/// Elsewhere it is Ctrl-C.
pub(crate) struct StopSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl StopSignals {
    /// Listens for the signals that stop the process, so one that comes from here on is not lost.
    ///
    /// # Errors
    ///
    /// [`ProxyError::NoStopSignal`] when the system lets the process listen for none.
    pub(crate) fn listen() -> Result<Self, ProxyError> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};

            let unheard = |error: std::io::Error| ProxyError::NoStopSignal {
                reason: error.to_string(),
            };
            Ok(Self {
                interrupt: signal(SignalKind::interrupt()).map_err(unheard)?,
                terminate: signal(SignalKind::terminate()).map_err(unheard)?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    /// Waits for the first signal that stops the process.
    pub(crate) async fn wait(mut self) {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.interrupt.recv() => {}
                _ = self.terminate.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            // A process that hears no Ctrl-C runs until something ends it.
            if tokio::signal::ctrl_c().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}
