use std::error::Error;
use std::fmt;
use tokio::sync::mpsc;
use tokio::time::Duration;

pub struct DeadlineClock {
    rx: mpsc::Receiver<()>,
    millis: u64,
}

#[derive(Clone)]
pub struct Reset(mpsc::Sender<()>);

#[derive(Debug)]
pub struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "deadline clock was disabled due to resetter drop")
    }
}

impl Error for Closed {}

pub fn clock(millis: u64) -> (DeadlineClock, Reset) {
    let (tx, rx) = mpsc::channel::<()>(1);

    let dc = DeadlineClock { rx, millis };
    let reset = Reset(tx);
    (dc, reset)
}

impl DeadlineClock {
    pub async fn run(mut self) -> Result<(), Closed> {
        loop {
            let d = Duration::from_millis(self.millis);
            let res = tokio::time::timeout(d, self.rx.recv()).await;
            match res {
                Ok(Some(())) => {
                    // reset
                }
                Ok(None) => {
                    // closed
                    break Err(Closed);
                }
                Err(_) => {
                    // timeout
                    break Ok(());
                }
            }
        }
    }
}

impl Reset {
    pub async fn reset(&mut self) -> Result<(), mpsc::error::SendError<()>> {
        self.0.send(()).await
    }
}

/*
#[cfg(test)]
mod test {
    use super::*;
    use tokio::time::{advance, pause, resume};

    #[tokio::test]
    async fn can_recv_deadline() {
        pause();
        let (tx, mut rx) = mpsc::channel(5);
        let (dc, mut r) = clock(1000, tx, ());
        tokio::spawn(dc.run());
        advance(Duration::from_millis(1001)).await;
        // r.reset().await;
        rx.recv().await;
    }

    #[tokio::test]
    async fn can_reset() {
        let (tx, mut rx) = mpsc::channel(1);
        let (dc, mut r) = clock(1000, tx, ());
        pause();
        tokio::spawn(dc.run());

        advance(Duration::from_millis(1100)).await;
        r.reset().await.unwrap();
        advance(Duration::from_millis(501)).await;

        assert!(rx.try_recv().is_err());
        rx.close();
    }

    #[tokio::test]
    async fn test_timeout() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        pause();
        let task = tokio::spawn(tokio::time::timeout(Duration::from_millis(5000), rx));
        advance(Duration::from_millis(5001)).await;
        let result = task.await;

        assert!(result.map(|r| r.is_err()).unwrap_or(false));
    }
}
*/
