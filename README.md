# fx-pgmux

A multiplexer for [`sqlx::PgListener`](https://docs.rs/sqlx/latest/sqlx/postgres/struct.PgListener.html) that lets multiple independent consumers share a single PostgreSQL `LISTEN` connection.

## Use-case

`sqlx::PgListener` requires one database connection per listener. If multiple parts of your application each need to react to `NOTIFY` events — on the same or different channels — each would normally consume its own connection. `fx-pgmux` routes incoming notifications from a single `PgListener` to any number of async `Stream`s in-process, keeping your connection count low regardless of how many consumers you have.

## Example

```rust
use fx_pgmux::Multiplexer;
use futures::StreamExt;
use sqlx::PgPool;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = PgPool::connect("postgres://postgres:postgres@localhost:5432/mydb").await?;

    let mut mux = Multiplexer::new(&pool).await?;

    // Register independent consumers — one connection, two streams
    let mut orders_stream = mux.register("orders").await?;
    let mut payments_stream = mux.register("payments").await?;

    // Hand the multiplexer off to a background task
    tokio::spawn(async move { mux.listen().await });

    // Each stream independently receives its channel's notifications
    tokio::spawn(async move {
        while let Some(payload) = orders_stream.next().await {
            println!("order event: {payload}");
        }
    });

    while let Some(payload) = payments_stream.next().await {
        println!("payment event: {payload}");
    }

    Ok(())
}
```

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.
