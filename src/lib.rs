use futures::{Stream, StreamExt, channel::mpsc};
use sqlx::{PgPool, postgres::PgListener};
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

pub struct Multiplexer {
    pg_listener: PgListener,
    channels: HashMap<&'static str, Vec<mpsc::UnboundedSender<String>>>,
}

pub struct NotificationStream {
    rx: mpsc::UnboundedReceiver<String>,
}

impl Stream for NotificationStream {
    type Item = String;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let s = self.get_mut();

        s.rx.poll_next_unpin(cx)
    }
}

impl Multiplexer {
    pub async fn new(pool: &PgPool) -> Result<Self, Error> {
        let pg_listener = PgListener::connect_with(pool).await?;

        Ok(Self {
            pg_listener,
            channels: HashMap::new(),
        })
    }

    pub async fn register(&mut self, channel: &'static str) -> Result<NotificationStream, Error> {
        let (tx, rx) = mpsc::unbounded();

        let entry = self.channels.entry(channel).or_default();

        if entry.is_empty() {
            self.pg_listener.listen(channel).await?;
        }

        entry.push(tx);

        Ok(NotificationStream { rx })
    }

    pub async fn listen(mut self) -> Result<(), Error> {
        loop {
            // Propagate errors - the listener should already handle reconnection internally.
            // If an error is received here its likely not easily recoverable
            let n = self.pg_listener.recv().await?;

            let channel = n.channel();
            let payload = n.payload();

            if let Some(senders) = self.channels.get_mut(channel) {
                senders.retain(|tx| {
                    if tx.unbounded_send(payload.to_string()).is_err() {
                        tracing::warn!(
                            message = "pgmux failed to send notification",
                            channel = channel,
                            payload = payload
                        );
                        return false;
                    }
                    true
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::PgPool;

    #[sqlx::test(migrations = false)]
    async fn single_consumer_receives_notification(pool: PgPool) -> anyhow::Result<()> {
        let mut mux = Multiplexer::new(&pool).await?;
        let mut stream = mux.register("test_channel").await?;

        tokio::spawn(async move { mux.listen().await });

        sqlx::query("SELECT pg_notify('test_channel', 'hello')")
            .execute(&pool)
            .await?;

        let msg = stream.next().await.unwrap();
        assert_eq!(msg, "hello");

        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn multiple_consumers_both_receive_notification(pool: PgPool) -> anyhow::Result<()> {
        let mut mux = Multiplexer::new(&pool).await?;
        let mut stream_a = mux.register("test_channel").await?;
        let mut stream_b = mux.register("test_channel").await?;

        tokio::spawn(async move { mux.listen().await });

        sqlx::query("SELECT pg_notify('test_channel', 'hello')")
            .execute(&pool)
            .await?;

        assert_eq!(stream_a.next().await.unwrap(), "hello");
        assert_eq!(stream_b.next().await.unwrap(), "hello");

        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn notifications_routed_to_correct_channel(pool: PgPool) -> anyhow::Result<()> {
        let mut mux = Multiplexer::new(&pool).await?;
        let mut stream_a = mux.register("channel_a").await?;
        let mut stream_b = mux.register("channel_b").await?;

        tokio::spawn(async move { mux.listen().await });

        sqlx::query("SELECT pg_notify('channel_a', 'for_a')")
            .execute(&pool)
            .await?;
        sqlx::query("SELECT pg_notify('channel_b', 'for_b')")
            .execute(&pool)
            .await?;

        assert_eq!(stream_a.next().await.unwrap(), "for_a");
        assert_eq!(stream_b.next().await.unwrap(), "for_b");

        Ok(())
    }

    #[sqlx::test(migrations = false)]
    async fn dropped_consumer_does_not_affect_others(pool: PgPool) -> anyhow::Result<()> {
        let mut mux = Multiplexer::new(&pool).await?;
        let mut stream_a = mux.register("test_channel").await?;
        let stream_b = mux.register("test_channel").await?;

        tokio::spawn(async move { mux.listen().await });

        // Drop one consumer
        drop(stream_b);

        sqlx::query("SELECT pg_notify('test_channel', 'hello')")
            .execute(&pool)
            .await?;

        // Remaining consumer still receives
        assert_eq!(stream_a.next().await.unwrap(), "hello");

        Ok(())
    }
}
