use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

#[tokio::main]
async fn main() {
    let (mut tx, mut rx) = mpsc::channel(100);

    tokio::spawn(async move {
        tx.send("hello").await.unwrap();
    });

    assert_eq!(Some("hello"), rx.recv().await);
    assert_eq!(None, rx.recv().await);
    assert_eq!(None, rx.recv().await);
    assert_eq!(
        Ok(None),
        timeout(Duration::from_millis(100), rx.recv()).await
    );
}
