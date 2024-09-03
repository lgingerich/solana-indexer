import json
import csv
# import base58
import base64
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solana.transaction import Signature
import asyncio
from construct import Struct, Bytes, Int64ul, Flag, Padding

# Constants
TRANSACTION_SIGNATURE = "5kHNF5yqpy65bMstNaTvA4oYtA6HTM6waSBDKdfqCW3DZ4VNetSMXbb9RHGEYdUfkKfmdMFAkwwRpujQL7Dsh915"
# IDL_PATH = "pumpfun_idl.json"
RAW_CSV_PATH = "raw_transaction_data.csv"
DECODED_CSV_PATH = "decoded_instructions.csv"

def load_idl(idl_path):
    with open(idl_path, 'r') as idl_file:
        return json.load(idl_file)

def decode_instruction(ix_data, idl):
    # Find the matching instruction in the IDL
    matching_ix = next((idl_ix for idl_ix in idl['instructions'] 
                        if idl_ix.get('discriminator', idl_ix.get('name', ''))[:8] == ix_data[:8].hex()), None)
    
    if matching_ix:
        # Create a Construct struct for decoding
        args_struct = Struct(
            "discriminator" / Bytes(8),
            *[
                (arg['name'], 
                 Int64ul if arg['type']['type'] == 'u64' else
                 Flag if arg['type']['type'] == 'bool' else
                 Bytes(int(arg['type']['type'].split('[')[1].split(']')[0])) if '[' in arg['type']['type'] else
                 Bytes(32))  # Default to 32 bytes for unknown types
                for arg in matching_ix['args']
            ],
            Padding(lambda ctx: len(ix_data) - ctx._subcons.sizeof())
        )

        try:
            # Decode the instruction data
            decoded_data = args_struct.parse(ix_data)
            return matching_ix['name'], {k: v.hex() if isinstance(v, bytes) else v for k, v in decoded_data.items() if k != 'discriminator'}
        except Exception as e:
            return matching_ix['name'], f"Decoding error: {str(e)}"
    else:
        return "Unknown instruction", None


async def fetch_and_decode_transaction(tx_signature_str, idl):
    async with AsyncClient("https://api.mainnet-beta.solana.com") as client:
        tx_signature = Signature.from_string(tx_signature_str)
        
        tx_resp = await client.get_transaction(
            tx_signature, 
            commitment=Confirmed, 
            max_supported_transaction_version=0
        )
        
        if not tx_resp or not tx_resp.value:
            print("Transaction not found")
            return None, []

        tx = tx_resp.value

        # Save raw transaction data to CSV
        with open(RAW_CSV_PATH, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Key', 'Value'])
            for key, value in tx.__dict__.items():
                writer.writerow([key, json.dumps(str(value))])

        decoded_instructions = []
        message = tx.transaction.message
        for idx, ix in enumerate(message.instructions):
            program_id = tx.transaction.message.account_keys[ix.program_id_index]
            ix_data = base64.b64decode(ix.data)
            ix_name, decoded_data = decode_instruction(ix_data, idl)
            
            decoded_instructions.append({
                'index': idx,
                'program_id': str(program_id),
                'instruction_name': ix_name,
                'decoded_data': decoded_data,
                'accounts': [str(tx.transaction.message.account_keys[i]) for i in ix.accounts]
            })

        # Save decoded instruction data to CSV
        with open(DECODED_CSV_PATH, 'w', newline='') as csvfile:
            fieldnames = ['index', 'program_id', 'instruction_name', 'decoded_data', 'accounts']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for ix in decoded_instructions:
                ix['decoded_data'] = json.dumps(ix['decoded_data'])
                ix['accounts'] = json.dumps(ix['accounts'])
                writer.writerow(ix)

        return tx, decoded_instructions

async def main():
    # idl = load_idl(IDL_PATH)
    tx, decoded_instructions = await fetch_and_decode_transaction(TRANSACTION_SIGNATURE, idl)
    
    if tx:
        print(f"Raw transaction data saved to {RAW_CSV_PATH}")
        print(f"Decoded instructions saved to {DECODED_CSV_PATH}")
        
        # Print decoded instructions
        for ix in decoded_instructions:
            print(f"\nInstruction {ix['index']}:")
            print(f"  Program ID: {ix['program_id']}")
            print(f"  Instruction Name: {ix['instruction_name']}")
            print(f"  Decoded Data: {ix['decoded_data']}")
            print(f"  Accounts: {ix['accounts']}")

if __name__ == "__main__":
    asyncio.run(main())