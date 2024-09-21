import os
from pathlib import Path
import polars as pl

def write_df_to_parquet(df: pl.DataFrame, data_type: str, slot: int, base_path: str = "data"):
    """
    Write a Polars DataFrame to a Parquet file.
    
    Args:
    df (pl.DataFrame): The DataFrame to write.
    data_type (str): The type of data (e.g., 'blocks', 'transactions', 'instructions', 'rewards').
    slot (int): The slot number for this data.
    base_path (str): The base directory to store the Parquet files.
    
    Returns:
    str: The path of the written Parquet file.
    """
    # Create the directory structure
    directory = Path(base_path) / data_type / f"slot_{slot // 1_000_000:07d}xxx"
    directory.mkdir(parents=True, exist_ok=True)
    
    # Create the file name
    file_name = f"{data_type}_slot_{slot:010d}.parquet"
    file_path = directory / file_name
    
    # Write the DataFrame to Parquet
    df.write_parquet(file_path)
    
    return str(file_path)

# You can add more data storage and handling functions here in the future