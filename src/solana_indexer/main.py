import asyncio
from indexer import SolanaIndexer
from utils import logger, load_config

async def main():
    # Load configuration and set up the Solana indexer
    config = await load_config()
    rpc_url = config["rpc"]["url"]
    start_slot = config["indexer"]["start_slot"]
    end_slot = config["indexer"].get("end_slot")
    
    # Log important configuration details
    logger.info(f"RPC URL: {rpc_url}")
    logger.info(f"Configured start slot: {start_slot}")
    logger.info(f"Configured end slot: {end_slot}")
    logger.info("Note: Actual starting slot may differ based on previously processed data. \n")

    # Initialize the Solana indexer with configured parameters
    indexer = SolanaIndexer(rpc_url, start_slot, end_slot)

    def signal_handler():
        """
        Handle keyboard interrupts (SIGINT) gracefully.
        This function is called when the user presses Ctrl+C.
        """
        logger.info("Keyboard interrupt received. Shutting down gracefully...")
        indexer.stop()

    try:
        # Set up the signal handler for graceful shutdown on SIGINT (Ctrl+C)
        asyncio.get_running_loop().add_signal_handler(
            asyncio.unix_events.signal.SIGINT, signal_handler
        )
    except NotImplementedError:
        # This exception might be raised on non-Unix systems where SIGINT handling is not supported
        pass

    # Start the indexer
    await indexer.run()

if __name__ == "__main__":
    # Run the main function asynchronously
    asyncio.run(main())