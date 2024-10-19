use std::path::PathBuf;
use mysql_async::prelude::*;
use clap::Parser;

mod owned_task;
mod front_node;
mod message;

#[derive(Parser)]
struct CLI {
    /// Path to config toml file
    #[arg(short='c', long="config-file")]
    config_file: PathBuf,
}

#[tokio::main]
async fn main() {
    let cli = CLI::parse();
    let cfg = front_node::config::Config::read_from_path(cli.config_file).await;
    eprintln!("Started with config:");
    eprintln!("{cfg:#?}");

    let _front_node = front_node::FrontNode::start_from_config(cfg).await.expect("could not start front node");

    loop {
        tokio::task::yield_now().await;
    }
}

