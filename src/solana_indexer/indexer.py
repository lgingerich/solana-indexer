import json
import polars as pl
import traceback

from schemas import SolanaSchemas
from utils import logger, async_retry

from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solana.exceptions import SolanaRpcException

class SolanaIndexer:
    def __init__(self, rpc_url):
        self.client = AsyncClient(rpc_url)
        self.latest_slot = None
        self.is_running = True
        self.schemas = SolanaSchemas()

    async def get_latest_slot(self):
        return await self.client.get_slot(Confirmed)
    
    @async_retry(retries=5, base_delay=1, exponential_backoff=True, jitter=True)
    async def process_block(self, slot):
        try:
            block = (await self.client.get_block(slot, encoding="json", max_supported_transaction_version=0)).value
            
            if block is None:
                raise Exception(f"Block not available for slot {slot}")

            logger.info(f"Processing block at slot {slot}")

            block_data = {
                "parent_slot": block.parent_slot,
                "slot": slot,
                "block_time": block.block_time,
                "block_height": block.block_height,
                "previous_blockhash": str(block.previous_blockhash),
                "blockhash": str(block.blockhash),
            }

            transactions_data = self.process_transactions(block.transactions, slot)
            instructions_data = self.process_instructions(block.transactions, slot)
            rewards_data = self.process_rewards(block.rewards, slot)

            return block_data, transactions_data, instructions_data, rewards_data
        
        except SolanaRpcException as e:
            logger.error(f"Solana RPC Exception in process_block for slot {slot}: {str(e)}")
            raise  # Re-raise the exception to be caught by the retry decorator

    def process_transactions(self, transactions, slot):
        transactions_data = []

        for tx in transactions:
            try:
                tx_data = {
                    "slot": slot,
                    "signature": str(tx.transaction.signatures[0]) if tx.transaction.signatures else None,
                    "num_required_signatures": tx.transaction.message.header.num_required_signatures if hasattr(tx.transaction.message, "header") else None,
                    "num_readonly_signed_accounts": tx.transaction.message.header.num_readonly_signed_accounts if hasattr(tx.transaction.message, "header") else None,
                    "num_readonly_unsigned_accounts": tx.transaction.message.header.num_readonly_unsigned_accounts if hasattr(tx.transaction.message, "header") else None,
                    "recent_blockhash": str(tx.transaction.message.recent_blockhash) if hasattr(tx.transaction.message, "recent_blockhash") else None,
                    "success": tx.meta.err is None if tx.meta else None,
                    "error": str(tx.meta.err) if tx.meta and tx.meta.err else None,
                    "fee": tx.meta.fee if tx.meta else None,
                    "pre_balances": json.dumps(tx.meta.pre_balances) if tx.meta and tx.meta.pre_balances else None,
                    "post_balances": json.dumps(tx.meta.post_balances) if tx.meta and tx.meta.post_balances else None,
                    "pre_token_balances": json.dumps(self.process_token_balances(tx.meta.pre_token_balances)) if tx.meta and hasattr(tx.meta, "pre_token_balances") else None, # this is often null — is that correct?
                    "post_token_balances": json.dumps(self.process_token_balances(tx.meta.post_token_balances)) if tx.meta and hasattr(tx.meta, "post_token_balances") else None, # this is often null — is that correct?
                    "log_messages": json.dumps(tx.meta.log_messages) if tx.meta and hasattr(tx.meta, "log_messages") else None,
                    "rewards": json.dumps(self.process_rewards(tx.meta.rewards, slot)) if tx.meta and hasattr(tx.meta, "rewards") else None,
                    "compute_units_consumed": tx.meta.compute_units_consumed if tx.meta and hasattr(tx.meta, "compute_units_consumed") else None,
                }
                transactions_data.append(tx_data)
            except Exception as e:
                logger.error(f"Error processing transaction in slot {slot}: {str(e)}")

        return transactions_data

    def process_instructions(self, transactions, slot):
        instructions_data = []

        for tx in transactions:
            try:
                for idx, instruction in enumerate(tx.transaction.message.instructions):
                    instruction_data = {
                        "slot": slot,
                        "tx_signature": str(tx.transaction.signatures[0]) if tx.transaction.signatures else None,
                        "instruction_index": idx,
                        "program_id_index": instruction.program_id_index,
                        "program_id": str(tx.transaction.message.account_keys[instruction.program_id_index]) if instruction.program_id_index < len(tx.transaction.message.account_keys) else None,
                        "accounts": json.dumps([str(tx.transaction.message.account_keys[i]) for i in instruction.accounts if i < len(tx.transaction.message.account_keys)]),
                        "data": instruction.data,
                        "is_inner": False
                    }
                    instructions_data.append(instruction_data)

                # Process inner instructions if available
                if tx.meta and hasattr(tx.meta, "inner_instructions"):
                    for inner_instructions in tx.meta.inner_instructions:
                        for inner_idx, inner_instruction in enumerate(inner_instructions.instructions):
                            inner_instruction_data = {
                                "slot": slot,
                                "tx_signature": str(tx.transaction.signatures[0]) if tx.transaction.signatures else None,
                                "instruction_index": inner_idx,
                                "parent_index": inner_instructions.index,
                                "program_id_index": inner_instruction.program_id_index,
                                "program_id": str(tx.transaction.message.account_keys[inner_instruction.program_id_index]) if inner_instruction.program_id_index < len(tx.transaction.message.account_keys) else None,
                                "accounts": json.dumps([str(tx.transaction.message.account_keys[i]) for i in inner_instruction.accounts if i < len(tx.transaction.message.account_keys)]),
                                "data": inner_instruction.data,
                                "is_inner": True
                            }
                            instructions_data.append(inner_instruction_data)
            except Exception as e:
                logger.error(f"Error processing instructions in slot {slot}: {str(e)}")

        return instructions_data

    def process_token_balances(self, token_balances):
        if not token_balances:
            return None
        
        processed_balances = []
        for balance in token_balances:
            try:
                processed_balance = {
                    "account_index": balance.account_index,
                    "mint": str(balance.mint),
                    "amount": balance.ui_token_amount.amount,
                    "decimals": balance.ui_token_amount.decimals,
                    "ui_amount": balance.ui_token_amount.ui_amount,
                    "ui_amount_string": balance.ui_token_amount.ui_amount_string,
                    "owner": str(balance.owner) if balance.owner else None,  # Convert Pubkey to string
                    "program_id": str(balance.program_id) if balance.program_id else None,  # Convert Pubkey to string
                }
                processed_balances.append(processed_balance)
            except Exception as e:
                logger.error(f"Error processing token balance: {str(e)}")
        return processed_balances

    def process_rewards(self, rewards, slot):
        if not rewards:
            return None
        
        processed_rewards = []
        for reward in rewards:
            try:
                processed_reward = {
                    "slot": slot,
                    "pubkey": str(reward.pubkey),
                    "lamports": reward.lamports,
                    "post_balance": reward.post_balance,
                    "reward_type": str(reward.reward_type),
                    "commission": reward.commission,
                }
                processed_rewards.append(processed_reward)
            except Exception as e:
                logger.error(f"Error processing reward in slot {slot}: {str(e)}")
        return processed_rewards

    async def run(self):
        try:
            while self.is_running:
                try:
                    if self.latest_slot is None:
                        self.latest_slot = (await self.get_latest_slot()).value
                    else:
                        slot = (await self.get_latest_slot()).value - 290824209
                        # slot = 287194310

                        logger.info(f"Processing slot: {slot}")

                        block_data, transactions_data, instructions_data, rewards_data = await self.process_block(slot)

                        # Convert data to Polars DataFrames
                        block_df = pl.DataFrame([block_data], schema=self.schemas.block_schema())
                        transactions_df = pl.DataFrame(transactions_data, schema=self.schemas.transactions_schema())
                        instructions_df = pl.DataFrame(instructions_data, schema=self.schemas.instructions_schema())
                        rewards_df = pl.DataFrame(rewards_data, schema=self.schemas.rewards_schema()) if rewards_data else None

                        # Print DataFrames (you can modify this to save to a file or database)
                        # print("Block DataFrame:")
                        # print(block_df)
                        # print("\nTransactions DataFrame:")
                        # print(transactions_df)
                        # print("\nInstructions DataFrame:")
                        # print(instructions_df)
                        # if rewards_df is not None:
                        #     print("\nRewards DataFrame:")
                        #     print(rewards_df)

                        # Increment the latest slot
                        self.latest_slot = slot + 1

                except SolanaRpcException as e:
                    logger.error(f"Solana RPC Exception in process_block for slot {slot}: {str(e)}")
                    raise  # Re-raise the exception to be caught by the retry decorator

                except Exception as e:
                    error_msg = f"Error in main loop: {str(e)}\n"
                    error_msg += "Traceback:\n"
                    error_msg += traceback.format_exc()
                    logger.error(error_msg)
        finally:
            await self.cleanup()

    async def cleanup(self):
        await self.client.close()
        logger.info("Indexer shut down successfully.")

    def stop(self):
        self.is_running = False