import asyncio
from indexer import SolanaIndexer
from utils import logger, load_config

async def main():
    # Setup configs
    config = await load_config()
    rpc_url = config["rpc"]["url"]
    start_slot = config["indexer"]["start_slot"]
    end_slot = config["indexer"].get("end_slot")
    
    logger.info(f"RPC URL: {rpc_url}")
    logger.info(f"Starting slot: {start_slot}")
    logger.info(f"Ending slot: {end_slot}")

    # Initialize indexer
    indexer = SolanaIndexer(rpc_url)

    def signal_handler():
        logger.info("Keyboard interrupt received. Shutting down gracefully...")
        indexer.stop()

    try:
        asyncio.get_running_loop().add_signal_handler(
            asyncio.unix_events.signal.SIGINT, signal_handler
        )
    except NotImplementedError:
        pass

    await indexer.run()

if __name__ == "__main__":
    asyncio.run(main())
