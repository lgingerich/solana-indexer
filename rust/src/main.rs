use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    clock::Slot,
    commitment_config::CommitmentConfig
};
use solana_transaction_status::UiConfirmedBlock;

const RPC_URL: &str = "https://api.mainnet-beta.solana.com";
const START_SLOT: Slot = 250_000_001;
// const START_SLOT: Slot = 2_001;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = RpcClient::new(RPC_URL);

    let block = process_block(client)?;

    println!("Block: {:?}", block); // normal print
    // println!("Block: {:#?}", block); // pretty print

    Ok(())
}

fn process_block(client: RpcClient) -> Result<UiConfirmedBlock, Box<dyn std::error::Error>> {
    let config = solana_client::rpc_config::RpcBlockConfig {
        encoding: Some(solana_transaction_status::UiTransactionEncoding::Json),
        transaction_details: Some(solana_transaction_status::TransactionDetails::Full),
        rewards: Some(true),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };

    let block: UiConfirmedBlock = client.get_block_with_config(START_SLOT, config)?;

    Ok(block)
}