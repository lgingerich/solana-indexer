import asyncio
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solders.signature import Signature
import json
from utils import logger
from solders.transaction_status import EncodedTransactionWithStatusMeta, UiTransactionStatusMeta
from solders.transaction import Transaction
import polars as pl


class SolanaIndexer:
    def __init__(self, rpc_url):
        self.client = AsyncClient(rpc_url)
        self.latest_slot = None
        self.is_running = True

    async def get_latest_slot(self):
        return await self.client.get_slot(Confirmed)
    
    async def process_block(self, slot):
        block = (await self.client.get_block(slot, encoding='json', max_supported_transaction_version=0)).value
        if block is None:
            raise Exception(f"Block not available for slot {slot}")

        logger.info(f"Processing block at slot {slot}")

        block_data = {
            "previous_blockhash": str(block.previous_blockhash),
            "blockhash": str(block.blockhash),
            "parent_slot": block.parent_slot,
            "slot": slot,
            "block_time": block.block_time, # early protocol version has nulls here
            "block_height": block.block_height # early protocol version has nulls here
        }
        transactions = block.transactions
        rewards = block.rewards

        return block_data, transactions, rewards

    async def process_transactions(self, transactions):
        transactions_data = []
        instructions = []

        # # for tx in transactions:
        tx = transactions[0]
        tx_data = {
            "signatures": [str(sig) for sig in tx.transaction.signatures],
            "num_required_signatures": tx.transaction.message.header.num_required_signatures,
            "num_readonly_signed_accounts": tx.transaction.message.header.num_readonly_signed_accounts,
            "num_readonly_unsigned_accounts": tx.transaction.message.header.num_readonly_unsigned_accounts,
            "account_keys": [str(account_key) for account_key in tx.transaction.message.account_keys],
            "recent_blockhash": str(tx.transaction.message.recent_blockhash),
            "version": tx.version,
            "error": str(tx.meta.err) if tx.meta and tx.meta.err else None,
            # "status": tx.status if tx.meta else None,
            "fee": tx.meta.fee if tx.meta else None,
            "pre_balances": tx.meta.pre_balances if tx.meta else None,
            "post_balances": tx.meta.post_balances if tx.meta else None,
        }
        
        transactions_data.append(tx_data)

        # Collect raw instructions without parsing
        instructions.extend(tx.transaction.message.instructions)
        
        return transactions_data, instructions


    async def process_instructions(self, instructions):
        pass


    async def run(self):
        try:
            while self.is_running:
                try:
                    if self.latest_slot is None:
                        self.latest_slot = (await self.get_latest_slot()).value
                    else:
                        slot = (await self.get_latest_slot()).value
                        logger.info(f"Current slot = {slot}")
                        
                        # slot = 100
                        # slot = 287194310
                        # slot = 1000000
                        slot -= 1000

                        block_data, transactions, rewards = await self.process_block(slot)
                        transactions_data, instructions = await self.process_transactions(transactions)
                        instructions_data = await self.process_instructions(instructions)
                    
                        # Convert data to Polars DataFrame and print
                        block_df = pl.DataFrame([block_data])  # Wrap in list to create a single-row DataFrame
                        print("Block DataFrame:")
                        print(block_df.head())

                        transactions_df = pl.DataFrame(transactions_data)
                        print("Transactions DataFrame:")
                        print(transactions_df.head())

                        # instructions_df = pl.DataFrame(instructions_data)
                        # print("Instructions DataFrame:")
                        # print(instructions_df.head())                        

                    await asyncio.sleep(1)  # Wait for 1 second before the next iteration

                except Exception as e:
                    logger.error(f"Error in main loop: {str(e)}")
                    await asyncio.sleep(5)  # Wait for 5 seconds before retrying
        finally:
            await self.cleanup()

    async def cleanup(self):
        # Close the client connection
        await self.client.close()
        logger.info("Indexer shut down successfully.")

    def stop(self):
        self.is_running = False

async def main():
    indexer = SolanaIndexer("https://api.mainnet-beta.solana.com")
    
    def signal_handler():
        logger.info("Keyboard interrupt received. Shutting down gracefully...")
        indexer.stop()

    try:
        asyncio.get_running_loop().add_signal_handler(
            asyncio.unix_events.signal.SIGINT, signal_handler)
    except NotImplementedError:
        pass

    await indexer.run()

if __name__ == "__main__":
    asyncio.run(main())