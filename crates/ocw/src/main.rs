mod cli;

#[tokio::main]
async fn main() {
    ocw_core::supervision::install_panic_hook();
    cli::main().await;
}
