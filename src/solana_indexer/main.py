import asyncio
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solders.signature import Signature
import json
from utils import logger


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
            signature = tx.transaction.signatures[0]

            try:
                # Fetch detailed transaction data using get_transaction()
                tx_response = (await self.client.get_transaction(signature)).value

                if tx_response is None:
                    logger.warning(f"Transaction data not available for signature: {signature}")
                    continue

                tx_data = {
                    "signature": str(signature),
                    "slot": tx_response.slot,
                    "block_time": tx_response.block_time,
                    "recent_blockhash": tx_response.transaction.transaction.message.recent_blockhash,
                    "fee": None,  # Fee information is not available in this structure
                    "status": "Success",  # Status information is not directly available
                    "err": None,  # Error information is not directly available
                    "accounts": [str(key) for key in tx_response.transaction.transaction.message.account_keys],
                    "instructions": [
                        {
                            "program_id_index": inst.program_id_index,
                            "accounts": inst.accounts, # need to decode this still
                            "data": inst.data,
                        } for inst in tx_response.transaction.transaction.message.instructions
                    ],
                    "log_messages": None,  # Log messages are not available in this structure
                }
                
                transactions_data.append(tx_data)

            except Exception as e:
                logger.error(f"Error processing transaction {signature}: {str(e)}")

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
                        
                        # slot = 100 # hardcode this for now to get shorter block response data
                        slot -= 100
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