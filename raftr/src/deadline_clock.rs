use std::future::Future;

use log::trace;
use tokio::sync::mpsc;
use tokio::time::Duration;

#[derive(Debug)]
pub struct Closed;

#[derive(Debug)]
pub struct ResetDeadline;

pub struct DeadlineClock {
    tx: mpsc::Sender<ResetDeadline>,
}

impl DeadlineClock {
    pub fn spawn<T>(millis: u64, task: T) -> Self
    where
        T: Future + Send + 'static,
    {
        let (tx, rx) = mpsc::channel::<ResetDeadline>(1);
        let process = DeadlineClockProcess { rx, millis };

        tokio::spawn(async move {
            if process.run().await.is_ok() {
                task.await;
            }
        });

        DeadlineClock { tx }
    }

    pub async fn reset_deadline(&mut self) -> Result<(), mpsc::error::SendError<ResetDeadline>> {
        self.tx.send(ResetDeadline).await
    }
}

pub struct DeadlineClockProcess {
    rx: mpsc::Receiver<ResetDeadline>,
    millis: u64,
}

impl DeadlineClockProcess {
    pub async fn run(mut self) -> Result<(), Closed> {
        loop {
            let d = Duration::from_millis(self.millis);
            let res = tokio::time::timeout(d, self.rx.recv()).await;
            match res {
                Ok(Some(ResetDeadline)) => {
                    // reset
                }
                Ok(None) => {
                    // closed
                    trace!("deadline clock process is terminated since the owner is dropped");
                    return Err(Closed);
                }
                Err(_) => {
                    // timeout
                    trace!(
                        "deadline clock process is terminated since the time reached the deadline"
                    );
                    return Ok(());
                }
            }
        }
    }
}
