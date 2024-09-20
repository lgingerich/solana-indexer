import asyncio
from indexer import SolanaIndexer
from utils import logger

async def main():
    indexer = SolanaIndexer("https://api.mainnet-beta.solana.com")

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
