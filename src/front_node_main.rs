use std::path::PathBuf;
use mysql_async::prelude::*;
use clap::Parser;

mod front_node;

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

    println!("mjau");

    let connection_options = cfg.connection_options.mysql_opts().await;
    let pool = mysql_async::Pool::new(connection_options);
    let mut transaction = pool.start_transaction(Default::default()).await.expect("Could not start transaction");

    let query = "SELECT user(), database();";

    eprintln!("querying {query}");

    let rows: Vec<(String, String)> = query.fetch(&mut transaction).await.expect("could not issue a SELECT");
    assert_eq!(rows.len(), 1);
    let (user, db) = &rows[0];
    eprintln!("connected as user={user:?} to db={db:?}");
}

