import asyncio
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solders.signature import Signature
import json
from utils import logger


#    "previous_blockhash":"AXu3HSYxcaQZbBsZJM98NiSWrQoHvunZmC2i5KF3pk1G",
#    "blockhash":"DdwYftNVqFsFUAfSqD59nT356KeSDfVJkUHjsGdvjR8y",
#    "parent_slot":99,
#    "block_time":"None",
#    "block_height":"None"

class SolanaIndexer:
    def __init__(self, rpc_url):
        self.client = AsyncClient(rpc_url)
        self.latest_slot = None
        self.is_running = True

    async def get_latest_slot(self):
        return await self.client.get_slot(Confirmed)
    
    async def process_block(self, slot):
        block = await self.client.get_block(slot, max_supported_transaction_version=0)
        if block is None:
            raise Exception(f"Block not available for slot {slot}")

        logger.info(f"Processing block at slot {slot}")

        return block

    async def run(self):
        try:
            while self.is_running:
                try:
                    if self.latest_slot is None:
                        self.latest_slot = (await self.get_latest_slot()).value
                    else:
                        slot = (await self.get_latest_slot()).value
                        logger.info(f"Current slot = {slot}")
                        
                        slot = 100 # hardcode this for now to get shorter block response
                        block = (await self.process_block(slot)).value

                        print(block)

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