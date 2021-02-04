use himalaya::himalaya_master_client::HimalayaMasterClient;
use himalaya::{DeleteRequest, Entry, GetRequest, PutRequest};

mod himalaya {
    tonic::include_proto!("himalaya");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = HimalayaMasterClient::connect("http://[::1]:50051").await?;

    {
        let request = tonic::Request::new(PutRequest {
            entry: Some(Entry {
                key: vec![0, 1, 2, 3],
                value: vec![0, 1, 2, 3],
            }),
        });

        let response = client.put(request).await?;

        println!("RESPONSE={:?}", response);
    }

    {
        let request = tonic::Request::new(GetRequest {
            key: vec![0, 1, 2, 3],
        });

        let response = client.get(request).await?;

        println!("RESPONSE={:?}", response);
    }

    {
        let request = tonic::Request::new(DeleteRequest {
            key: vec![0, 1, 2, 3],
        });

        let response = client.delete(request).await?;

        println!("RESPONSE={:?}", response);
    }
    Ok(())
}
