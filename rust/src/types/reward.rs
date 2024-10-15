use solana_sdk::clock::Slot;
use solana_transaction_status::{Reward, RewardType};

#[derive(Debug)]
pub struct RewardInfo {
    pub slot: Slot,
    pub pubkey: String,
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: RewardType,
    pub commission: Option<u8>,
}

impl RewardInfo {
    pub fn new(slot: Slot, reward: Reward) -> Self {
        RewardInfo { 
            slot,
            pubkey: reward.pubkey,
            lamports: reward.lamports,
            post_balance: reward.post_balance,
            reward_type: reward.reward_type.unwrap_or(RewardType::Fee),
            commission: reward.commission,
        }
    }
}