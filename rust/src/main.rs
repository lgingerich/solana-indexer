mod types;
use crate::types::{
    block::{Block, BlockInfo},
    reward::RewardInfo
};
use solana_client::{
    rpc_client::RpcClient,
    client_error::ClientError
};
use solana_sdk::{
    clock::Slot,
    commitment_config::CommitmentConfig
};
use solana_transaction_status::{
    UiConfirmedBlock,
    Rewards
};


const RPC_URL: &str = "https://api.mainnet-beta.solana.com";
const START_SLOT: Slot = 250_000_001;
// const START_SLOT: Slot = 2_001;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = RpcClient::new(RPC_URL);

    let block = get_block(client)?;
    process_block(START_SLOT, &block);
    process_reward(START_SLOT, block.rewards.as_ref());

    // println!("Block: {:?}", block); // normal print
    // println!("Block: {:#?}", block); // pretty print

    Ok(())
}

fn get_block(client: RpcClient) -> Result<UiConfirmedBlock, ClientError> {
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


fn process_block(slot: Slot, block: &Block) {
    let block_info = BlockInfo::new(slot, block);
    println!("Block Info: {:#?}", block_info);
}

// fn process_transaction() {

// }

// fn process_instruction() {

// }

fn process_reward(slot: Slot, rewards: Option<&Rewards>) {
    if let Some(rewards) = rewards {
        let reward_info: Vec<RewardInfo> = rewards.iter()
            .map(|reward| RewardInfo::new(slot, reward.clone()))
            .collect();
        println!("Rewards for slot {}: {:#?}", slot, reward_info);
    } else {
        println!("No rewards for slot {}", slot);
    }
}