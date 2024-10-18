import asyncio

from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solana.exceptions import SolanaRpcException

client = AsyncClient("https://api.mainnet-beta.solana.com")

async def test_func():
    # x = (await client.get_block(2500000, encoding="json", max_supported_transaction_version=0)).value
    x = (await client.get_block_time(290000000)).value
    print(x)

asyncio.run(test_func())

