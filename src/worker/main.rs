mod server;
use clap::{App, load_yaml};

fn main() {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from(yaml).get_matches();

    if let Some(ref _matches) = matches.subcommand_matches("start") {
        println!("Starting worker ...")
    }
}