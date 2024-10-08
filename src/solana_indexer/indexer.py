import asyncio
import json
import traceback
from typing import Any, Dict, List, Optional, Tuple

from data_manager import get_data_store
from utils import logger, async_retry

from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solana.exceptions import SolanaRpcException

class SolanaIndexer:
    """
    A class for indexing Solana blockchain data.
    This indexer processes blocks, transactions, instructions, and rewards,
    storing the data using the configured DataStore for efficient querying and analysis.
    """

    def __init__(self, rpc_url, start_slot, end_slot, data_store_type="parquet", data_store_params=None):
        """
        Initialize the SolanaIndexer with RPC connection, slot range, and data store configuration.
        
        :param rpc_url: URL for the Solana RPC node
        :param start_slot: Starting slot for indexing (can be 'latest', 'genesis', or a specific slot number)
        :param end_slot: Ending slot for indexing (or None for continuous indexing)
        :param data_store_type: Type of data store to use (e.g., 'parquet', 'iceberg')
        :param data_store_params: Additional parameters for the data store
        """
        self.client = AsyncClient(rpc_url)
        self.configured_start_slot = start_slot
        self.end_slot = end_slot
        self.current_slot = None
        self.latest_confirmed_slot = None
        self.is_running = True
        self.data_store = get_data_store(data_store_type, **(data_store_params or {}))

    async def initialize(self) -> None:
        """
        Initialize the indexer by determining the starting slot.
        This method handles different start configurations and resuming from previously processed data.
        """
        self.latest_confirmed_slot = await self.get_latest_slot()
        last_processed_slot = self.data_store.find_last_processed_block()

        if last_processed_slot is not None:
            # Resume from the next slot after the last processed one
            self.current_slot = last_processed_slot + 1
            logger.info(f"Found previously processed data. Resuming from slot: {self.current_slot}")
        elif self.configured_start_slot == "latest":
            self.current_slot = self.latest_confirmed_slot
            logger.info(f"No previous data found. Starting from latest confirmed slot: {self.current_slot}")
        elif self.configured_start_slot == "genesis":
            self.current_slot = 0
            logger.info("No previous data found. Starting from genesis block (slot 0)")
        else:
            self.current_slot = self.configured_start_slot
            logger.info(f"No previous data found. Starting from configured slot: {self.current_slot}")

        logger.info(f"Indexer initialized. Processing will begin at slot: {self.current_slot}")

    async def get_latest_slot(self) -> int:
        """Fetch the latest confirmed slot from the Solana network."""
        return (await self.client.get_slot(Confirmed)).value
    
    @async_retry(retries=5, base_delay=1, exponential_backoff=True, jitter=True)
    async def process_block(self, slot: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Optional[List[Dict[str, Any]]]]:
        """
        Process a single block at the given slot.
        This method fetches the block data and extracts relevant information.
        
        :param slot: The slot number of the block to process
        :return: Tuple of (block_data, transactions_data, instructions_data, rewards_data)
        """
        try:
            block = (await self.client.get_block(slot, encoding="json", max_supported_transaction_version=0)).value
            
            if block is None:
                raise Exception(f"Block not available for slot {slot}")

            logger.info(f"Processing block at slot {slot}")

            # Extract basic block information
            block_data = [{
                "slot": slot,
                "parent_slot": block.parent_slot,
                "block_time": block.block_time,
                "block_height": block.block_height,
                "previous_blockhash": str(block.previous_blockhash),
                "blockhash": str(block.blockhash),
            }]

            # Process transactions, instructions, and rewards
            transactions_data = self.process_transactions(block.transactions, slot)
            instructions_data = self.process_instructions(block.transactions, slot)
            rewards_data = self.process_rewards(block.rewards, slot)

            return block_data, transactions_data, instructions_data, rewards_data
        
        except SolanaRpcException as e:
            logger.error(f"Solana RPC Exception in process_block for slot {slot}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in process_block for slot {slot}: {str(e)}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            raise

    def process_transactions(self, transactions: List[Any], slot: int) -> List[Dict[str, Any]]:
        """
        Process all transactions in a block.
        
        :param transactions: List of transactions in the block
        :param slot: The slot number of the block
        :return: List of processed transaction data
        """
        transactions_data = []

        for tx in transactions:
            try:
                # Extract relevant transaction information
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
                    "pre_token_balances": json.dumps(self.process_token_balances(tx.meta.pre_token_balances)) if tx.meta and hasattr(tx.meta, "pre_token_balances") else None,
                    "post_token_balances": json.dumps(self.process_token_balances(tx.meta.post_token_balances)) if tx.meta and hasattr(tx.meta, "post_token_balances") else None,
                    "log_messages": json.dumps(tx.meta.log_messages) if tx.meta and hasattr(tx.meta, "log_messages") else None,
                    "rewards": json.dumps(self.process_rewards(tx.meta.rewards, slot)) if tx.meta and hasattr(tx.meta, "rewards") else None,
                    "compute_units_consumed": tx.meta.compute_units_consumed if tx.meta and hasattr(tx.meta, "compute_units_consumed") else None,
                }
                transactions_data.append(tx_data)
            except Exception as e:
                logger.error(f"Error processing transaction in slot {slot}: {str(e)}")
                logger.debug(f"Traceback: {traceback.format_exc()}")

        return transactions_data

    def process_instructions(self, transactions: List[Any], slot: int) -> List[Dict[str, Any]]:
        """
        Process all instructions in a block's transactions.
        This includes both top-level and inner instructions.
        
        :param transactions: List of transactions in the block
        :param slot: The slot number of the block
        :return: List of processed instruction data
        """
        instructions_data = []

        for tx in transactions:
            try:
                # Process top-level instructions
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
                logger.debug(f"Traceback: {traceback.format_exc()}")

        return instructions_data

    def process_token_balances(self, token_balances: Optional[List[Any]]) -> Optional[List[Dict[str, Any]]]:
        """
        Process token balance information from transactions.
        
        :param token_balances: List of token balance objects
        :return: List of processed token balance data
        """
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
                    "owner": str(balance.owner) if balance.owner else None,
                    "program_id": str(balance.program_id) if balance.program_id else None,
                }
                processed_balances.append(processed_balance)
            except Exception as e:
                logger.error(f"Error processing token balance: {str(e)}")
                logger.debug(f"Traceback: {traceback.format_exc()}")

        return processed_balances

    def process_rewards(self, rewards: Optional[List[Any]], slot: int) -> Optional[List[Dict[str, Any]]]:
        """
        Process reward information from blocks or transactions.
        
        :param rewards: List of reward objects
        :param slot: The slot number associated with the rewards
        :return: List of processed reward data
        """
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
                logger.debug(f"Traceback: {traceback.format_exc()}")

        return processed_rewards

    async def run(self) -> None:
        """
        Main loop for the indexer. This method continuously processes blocks
        until the end slot is reached or the indexer is stopped.
        """
        try:
            await self.initialize()
            while self.is_running:
                try:
                    # Check if we've reached the end slot
                    if self.end_slot is not None and self.current_slot > self.end_slot:
                        logger.info(f"Reached end slot {self.end_slot}. Stopping indexer.")
                        break

                    # Wait for new blocks if we've caught up to the latest
                    if self.current_slot >= self.latest_confirmed_slot:
                        self.latest_confirmed_slot = await self.get_latest_slot()
                        if self.current_slot >= self.latest_confirmed_slot:
                            await asyncio.sleep(1)  # Wait for new blocks
                            continue

                    logger.info(f"Processing slot: {self.current_slot}")

                    # Process the current block
                    block_data, transactions_data, instructions_data, rewards_data = await self.process_block(self.current_slot)

                    # Write DataFrames using the configured data store
                    try:
                        self.data_store.write_table(block_data, "blocks", self.current_slot)
                        self.data_store.write_table(transactions_data, "transactions", self.current_slot)
                        self.data_store.write_table(instructions_data, "instructions", self.current_slot)
                        if rewards_data is not None:
                            self.data_store.write_table(rewards_data, "rewards", self.current_slot)
                    except Exception as e:
                        logger.error(f"Error writing data for slot {self.current_slot}: {str(e)}")
                        logger.debug(f"Traceback: {traceback.format_exc()}")

                    # Move to the next slot
                    self.current_slot += 1

                except SolanaRpcException as e:
                    logger.error(f"Solana RPC Exception in run loop for slot {self.current_slot}: {str(e)}")
                    await asyncio.sleep(5)  # Add a delay before retrying
                except Exception as e:
                    logger.error(f"Unexpected error in run loop for slot {self.current_slot}: {str(e)}")
                    logger.debug(f"Traceback: {traceback.format_exc()}")
                    await asyncio.sleep(5)  # Add a delay before retrying
        except Exception as e:
            logger.critical(f"Critical error in run method: {str(e)}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
        finally:
            await self.cleanup()

    async def cleanup(self) -> None:
        """Perform cleanup operations when the indexer is shutting down."""
        await self.client.close()
        logger.info("Indexer shut down successfully.")

    def stop(self) -> None:
        """Signal the indexer to stop processing."""
        self.is_running = False