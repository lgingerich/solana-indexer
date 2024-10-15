use solana_sdk::clock::Slot;
use solana_transaction_status::UiConfirmedBlock;

pub type Block = UiConfirmedBlock;

#[derive(Debug)]
pub struct BlockInfo {
    pub slot: Slot,
    pub parent_slot: Slot,
    pub previous_blockhash: String,
    pub blockhash: String,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
}

impl BlockInfo {
    pub fn new(slot: Slot, block: &UiConfirmedBlock) -> Self {
        BlockInfo {
            slot,
            parent_slot: block.parent_slot,
            previous_blockhash: block.previous_blockhash.clone(),
            blockhash: block.blockhash.clone(),
            block_time: block.block_time,
            block_height: block.block_height,
        }
    }
}