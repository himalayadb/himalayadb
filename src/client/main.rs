use clap::{load_yaml, App, AppSettings};
use himalaya::proto::himalaya::himalaya_client::HimalayaClient;
use himalaya::proto::himalaya::{GetRequest, PutRequest};

enum Operation<'b> {
    Read { key: &'b [u8] },
    Write { key: &'b [u8], value: &'b [u8] },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("options.yaml");
    let matches = App::from(yaml)
        .setting(AppSettings::AllowNegativeNumbers)
        .get_matches();

    let host = matches.value_of("host").expect("You must provide a host.");
    let port = matches
        .value_of("port")
        .expect("You must provide a host.")
        .parse::<usize>()
        .unwrap();

    let command = if let Some(ref matches) = matches.subcommand_matches("read") {
        Operation::Read {
            key: matches
                .value_of("key")
                .expect("You must provide a key.")
                .as_bytes(),
        }
    } else if let Some(ref matches) = matches.subcommand_matches("write") {
        Operation::Write {
            key: matches
                .value_of("key")
                .expect("You must provide a key.")
                .as_bytes(),
            value: matches
                .value_of("value")
                .expect("You must provide a value.")
                .as_bytes(),
        }
    } else {
        panic!("You must provide a command.");
    };

    let host = format!("http://{}:{}", host, port);

    let mut client = HimalayaClient::connect(host).await?;
    match command {
        Operation::Read { key } => {
            let request = tonic::Request::new(GetRequest { key: key.to_vec() });
            let response = client.get(request).await?;
            println!("RESPONSE={:?}", response);
        }
        Operation::Write { key, value } => {
            let request = tonic::Request::new(PutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
            });

            let response = client.put(request).await?;
            println!("RESPONSE={:?}", response);
        }
    }

    Ok(())
}
