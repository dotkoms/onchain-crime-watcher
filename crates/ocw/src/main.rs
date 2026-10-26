mod cli;

#[tokio::main]
async fn main() {
    cli::main().await;
}
