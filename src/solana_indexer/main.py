import asyncio
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solders.signature import Signature
import json
from utils import logger
from solders.transaction_status import EncodedTransactionWithStatusMeta
from solders.transaction import Transaction

class SolanaIndexer:
    def __init__(self, rpc_url):
        self.client = AsyncClient(rpc_url)
        self.latest_slot = None
        self.is_running = True

    async def get_latest_slot(self):
        return await self.client.get_slot(Confirmed)
    
    async def process_block(self, slot):
        block = (await self.client.get_block(slot, max_supported_transaction_version=0)).value
        if block is None:
            raise Exception(f"Block not available for slot {slot}")

        logger.info(f"Processing block at slot {slot}")

        block_data = {
            "previous_blockhash": block.previous_blockhash,
            "blockhash": block.blockhash,
            "parent_slot": block.parent_slot,
            "slot": slot,
            "transactions": block.transactions,
            "rewards": block.rewards,
            "block_time": block.block_time,
            "block_height": block.block_height
        }

        return block_data


    async def process_txs(self, block):
        transactions_data = []

        for _, tx in enumerate(block['transactions']):
            tx_data = {
                "transaction": {
                    "signatures": tx.transaction.signatures,
                    "message": {
                        "header": {
                            "num_required_signatures": tx.transaction.message.header.num_required_signatures,
                            "num_readonly_signed_accounts": tx.transaction.message.header.num_readonly_signed_accounts,
                            "num_readonly_unsigned_accounts": tx.transaction.message.header.num_readonly_unsigned_accounts
                        },
                        "account_keys": tx.transaction.message.account_keys,
                        "recent_blockhash": tx.transaction.message.recent_blockhash,
                        "instructions": [
                            {
                                "program_id_index": inst.program_id_index,
                                "accounts": inst.accounts,
                                "data": inst.data,
                                "stack_height": getattr(inst, 'stack_height', None), # use getattr to handle failures on fetching
                            } for inst in tx.transaction.message.instructions
                        ],
                        "address_table_lookups": tx.transaction.message.address_table_lookups,
                    }
                },
                "meta": tx.meta,
                "version": tx.version
            }
            print(tx_data)
            transactions_data.append(tx_data)

            await asyncio.sleep(1)
        
        return transactions_data


    async def run(self):
        try:
            while self.is_running:
                try:
                    if self.latest_slot is None:
                        self.latest_slot = (await self.get_latest_slot()).value
                    else:
                        slot = (await self.get_latest_slot()).value
                        logger.info(f"Current slot = {slot}")
                        
                        slot = 100 # hardcode this for now to get shorter block response data
                        # slot -= 100
                        block_data = await self.process_block(slot)
                        transaction_data = await self.process_txs(block_data)

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