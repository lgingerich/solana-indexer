import pytest
import polars as pl
from pathlib import Path
from src.solana_indexer.data_store import write_df_to_parquet, find_last_processed_block

@pytest.fixture
def temp_data_dir(tmp_path):
    return tmp_path / "data"

def test_write_df_to_parquet(temp_data_dir):
    df = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    data_type = "blocks"
    slot = 1000000

    file_path = write_df_to_parquet(df, data_type, slot, base_path=str(temp_data_dir))

    assert Path(file_path).exists()
    assert Path(file_path).name == "blocks_slot_0001000000.parquet"
    # Fix: Update the expected parent directory name
    assert Path(file_path).parent.name == "slot_0000001000xxx"

def test_write_df_to_parquet_invalid_inputs(temp_data_dir):
    with pytest.raises(ValueError):
        write_df_to_parquet("not_a_dataframe", "blocks", 1000000, base_path=str(temp_data_dir))
    
    with pytest.raises(ValueError):
        write_df_to_parquet(pl.DataFrame(), "", 1000000, base_path=str(temp_data_dir))
    
    with pytest.raises(ValueError):
        write_df_to_parquet(pl.DataFrame(), "blocks", -1, base_path=str(temp_data_dir))

def test_find_last_processed_block_empty_dir(temp_data_dir):
    assert find_last_processed_block(base_path=str(temp_data_dir)) is None

def test_find_last_processed_block(temp_data_dir):
    df = pl.DataFrame({"col1": [1]})
    write_df_to_parquet(df, "blocks", 1000000, base_path=str(temp_data_dir))
    write_df_to_parquet(df, "transactions", 1000001, base_path=str(temp_data_dir))
    write_df_to_parquet(df, "instructions", 1000002, base_path=str(temp_data_dir))

    assert find_last_processed_block(base_path=str(temp_data_dir)) == 1000002

def test_find_last_processed_block_with_gaps(temp_data_dir):
    df = pl.DataFrame({"col1": [1]})
    write_df_to_parquet(df, "blocks", 1000000, base_path=str(temp_data_dir))
    write_df_to_parquet(df, "transactions", 1000005, base_path=str(temp_data_dir))
    write_df_to_parquet(df, "instructions", 1000002, base_path=str(temp_data_dir))

    assert find_last_processed_block(base_path=str(temp_data_dir)) == 1000005

def test_find_last_processed_block_invalid_filenames(temp_data_dir):
    df = pl.DataFrame({"col1": [1]})
    write_df_to_parquet(df, "blocks", 1000000, base_path=str(temp_data_dir))
    
    # Create an invalid filename
    invalid_file = temp_data_dir / "blocks" / "slot_0001000xxx" / "invalid_file.parquet"
    invalid_file.parent.mkdir(parents=True, exist_ok=True)
    invalid_file.touch()

    assert find_last_processed_block(base_path=str(temp_data_dir)) == 1000000
