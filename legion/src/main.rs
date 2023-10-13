mod migrate;
mod server;

use clap::{Parser, Subcommand};
use log::info;



#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>
}

#[derive(Subcommand)]
enum Commands {
    /// Migrate postgres database to the latest version of Legion schema
    Migrate {
        /// lists test values
        #[arg(short, long)]
        postgres_url: Option<String>,
    },

    /// Run the Legion server
    Start { 
        /// Queue to subscribe to for background jobs
        #[arg(short, long)]
        queue_name: String,
        
        /// Entrypoint for the background job 
        #[arg(short, long)]
        command: String,

        #[arg(short, long)]
        postgres_url: String,
    }
}

fn load_postgres_url(postgres_url: &Option<String>) -> String {
    match postgres_url {
        Some(_) => postgres_url.clone().unwrap(),
        None => {
            match std::env::var("DATABASE_URL") {
                Ok(url) => url,
                Err(_) => {
                    println!("No postgres_url or DATABASE_URL environment variable set");
                    std::process::exit(1);
                }
            }
        },
    }
}

fn main() {
    env_logger::init();

    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Migrate { postgres_url }) => {
            let url = load_postgres_url(postgres_url);
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(migrate::migrate(url))
        }
        Some(Commands::Start { 
            postgres_url,
            queue_name: queue, 
            command
         }) => {
            info!("Starting Legion server");

            let rt = tokio::runtime::Runtime::new().unwrap();
            match rt.block_on(server::start(postgres_url.clone(), queue.clone(), command.clone())) {
                Ok(_) => {}
                Err(e) => {
                    info!("Error running server: {:?}", e);
                    std::process::exit(1);
                }
            }
         }
        None => {}
    }
}