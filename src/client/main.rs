mod himalaya {
    tonic::include_proto!("himalaya");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client =
        himalaya::himalaya_client::HimalayaClient::connect("http://[::1]:50051").await?;

    {
        let request = tonic::Request::new(himalaya::PutRequest {
            key: vec![0, 1, 2, 3],
            value: vec![0, 1, 2, 3],
        });

        let response = client.put(request).await?;

        println!("RESPONSE={:?}", response);
    }

    {
        let request = tonic::Request::new(himalaya::GetRequest {
            key: vec![0, 1, 2, 3],
        });

        let response = client.get(request).await?;

        println!("RESPONSE={:?}", response);
    }

    {
        let request = tonic::Request::new(himalaya::DeleteRequest {
            key: vec![0, 1, 2, 3],
        });

        let response = client.delete(request).await?;

        println!("RESPONSE={:?}", response);
    }
    Ok(())
}
