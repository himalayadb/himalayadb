use clap::{App, load_yaml, AppSettings};
use tokio::sync::oneshot;

mod himalaya {
    tonic::include_proto!("himalaya");
}

enum Operation {
    Read{
        key: Vec<u8>
    },
    Write {
        key: Vec<u8>,
        value: Vec<u8>
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("options.yaml");
    let matches = App::from(yaml).setting(AppSettings::AllowNegativeNumbers).get_matches();

    let host = matches.value_of("host").expect("You must provide a host.");
    let port = matches.value_of("port").expect("You must provide a host.").parse::<usize>().unwrap();


    let mut command: Operation;

    if let Some(ref matches) = matches.subcommand_matches("read") {
        command = Operation::Read {
            key: matches.value_of("key").expect("You must provide a key.").as_bytes().to_vec()
        }
    } else if let Some(ref matches) = matches.subcommand_matches("write") {
        command = Operation::Write {
            key: matches.value_of("key").expect("You must provide a key.").as_bytes().to_vec(),
            value: matches.value_of("value").expect("You must provide a value.").as_bytes().to_vec(),
        }
    } else {
        panic!("You must provide a command.");
    }

    let host = format!("http://{}:{}", host, port);

    let mut client =
        himalaya::himalaya_client::HimalayaClient::connect(host).await?;
    {
        match command {
            Operation::Read {key} => {
                let request = tonic::Request::new(himalaya::GetRequest { key });
                let response = client.get(request).await?;
                println!("RESPONSE={:?}", response);
            },
            Operation::Write {key, value} => {
                let request = tonic::Request::new(himalaya::PutRequest {
                    key,
                    value,
                });

                let response = client.put(request).await?;
                println!("RESPONSE={:?}", response);
            },
        }
    }

    Ok(())
}
