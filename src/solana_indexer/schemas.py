import polars as pl

class SolanaSchemas:
    @staticmethod
    def block_schema():
        return {
            "parent_slot": pl.Int64,
            "slot": pl.Int64,
            "block_time": pl.Int64,
            "block_height": pl.Int64,
            "previous_blockhash": pl.Utf8,
            "blockhash": pl.Utf8,
        }

    @staticmethod
    def transactions_schema():
        return {
            "slot": pl.Int64,
            "signature": pl.Utf8,
            "num_required_signatures": pl.Int64,
            "num_readonly_signed_accounts": pl.Int64,
            "num_readonly_unsigned_accounts": pl.Int64,
            "recent_blockhash": pl.Utf8,
            "success": pl.Boolean,
            "error": pl.Utf8,
            "fee": pl.Int64,
            "pre_balances": pl.Utf8,
            "post_balances": pl.Utf8,
            "pre_token_balances": pl.Utf8,
            "post_token_balances": pl.Utf8,
            "log_messages": pl.Utf8,
            "rewards": pl.Utf8,
            "compute_units_consumed": pl.Int64,
        }

    @staticmethod
    def instructions_schema():
        return {
            "slot": pl.Int64,
            "tx_signature": pl.Utf8,
            "instruction_index": pl.Int64,
            "program_id_index": pl.Int64,
            "program_id": pl.Utf8,
            "accounts": pl.Utf8,
            "data": pl.Utf8,
            "is_inner": pl.Boolean,
            "parent_index": pl.Int64,
        }

    @staticmethod
    def rewards_schema():
        return {
            "slot": pl.Int64,
            "pubkey": pl.Utf8,
            "lamports": pl.Int64,
            "post_balance": pl.Int64,
            "reward_type": pl.Utf8,
            "commission": pl.Int64,
        }
    
    # token_balances_schema = {
    #     "account_index": pl.Int64,
    #     "mint": pl.Utf8,
    #     "amount": pl.Utf8,  # Using Utf8 as amount can be large
    #     "decimals": pl.Int64,
    #     "ui_amount": pl.Float64,
    #     "ui_amount_string": pl.Utf8,
    #     "owner": pl.Utf8,
    #     "program_id": pl.Utf8,
    # }