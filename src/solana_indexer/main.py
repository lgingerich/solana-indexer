import asyncio
from indexer import SolanaIndexer
from utils import logger, load_config


# Load configs
config = load_config()

if config:
    rpc_url = config["rpc"]["url"]
    start_block = config["indexer"]["start_block"]
    
    logger.info(f"RPC URL: {rpc_url}")
    logger.info(f"Starting block: {start_block}")
else:
    logger.error("Failed to load configuration")

async def main():
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
