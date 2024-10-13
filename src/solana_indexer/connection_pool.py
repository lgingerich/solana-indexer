import asyncio
from typing import List
from solana.rpc.async_api import AsyncClient

class AsyncClientPool:
    def __init__(self, rpc_url: str, pool_size: int = 10):
        self.rpc_url = rpc_url
        self.pool_size = pool_size
        self.clients: List[AsyncClient] = []
        self.semaphore = asyncio.Semaphore(pool_size)
        self.lock = asyncio.Lock()

    async def initialize(self):
        async with self.lock:
            if not self.clients:
                self.clients = [AsyncClient(self.rpc_url) for _ in range(self.pool_size)]

    async def get_client(self):
        await self.initialize()
        await self.semaphore.acquire()
        return self.clients.pop()

    async def release_client(self, client: AsyncClient):
        self.clients.append(client)
        self.semaphore.release()

    async def close(self):
        for client in self.clients:
            await client.close()
        self.clients.clear()

class AsyncClientContext:
    def __init__(self, pool: AsyncClientPool):
        self.pool = pool
        self.client = None

    async def __aenter__(self):
        self.client = await self.pool.get_client()
        return self.client

    async def __aexit__(self, exc_type, exc, tb):
        await self.pool.release_client(self.client)
