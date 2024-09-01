from solana.rpc.api import Client
from solana.rpc.commitment import Confirmed

http_client = Client("https://api.devnet.solana.com")


slot_number = http_client.get_slot(Confirmed).value
slot_number -= 100
block = http_client.get_block(slot_number, max_supported_transaction_version=0).value

print(slot_number)
print(block)
