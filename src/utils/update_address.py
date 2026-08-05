"""Update customer address and maintain address history."""
from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from src.utils.logging_setup import log_event


def update_customer_address(
    customer_id: str,
    new_city: str,
    new_country: str,
    data_path: Path,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Update a customer's address and add the old address to past_addresses.
    
    Args:
        customer_id: The customer ID to update
        new_city: The new city for the customer
        new_country: The new country code for the customer
        data_path: Path to the parquet file containing customer data
        logger: Optional logger instance for logging events
        
    Raises:
        ValueError: If customer_id is not found
        FileNotFoundError: If data_path does not exist
    """
    if not data_path.exists():
        raise FileNotFoundError(f"Customer data file not found: {data_path}")
    
    # Read the parquet file
    df = pd.read_parquet(data_path)
    
    # Find the customer
    customer_mask = df["customer_id"] == customer_id
    if not customer_mask.any():
        raise ValueError(f"Customer ID not found: {customer_id}")
    
    # Get the customer's current address
    customer_idx = df[customer_mask].index[0]
    old_city = df.loc[customer_idx, "city"]
    old_country = df.loc[customer_idx, "country"]
    
    # Format the old address as "[CITY] ([COUNTRY])"
    old_address = f"{old_city} ({old_country})"
    
    # Get current past_addresses list (or empty list if None)
    past_addresses = df.loc[customer_idx, "past_addresses"]
    if past_addresses is None:
        past_addresses = []
    elif hasattr(past_addresses, 'tolist'):
        # Handle numpy arrays from parquet
        past_addresses = past_addresses.tolist()
    elif isinstance(past_addresses, list):
        # Make a copy to avoid modifying the original
        past_addresses = list(past_addresses)
    else:
        past_addresses = []
    
    # Add the old address to past_addresses
    past_addresses.append(old_address)
    
    # Update the customer's record
    df.loc[customer_idx, "city"] = new_city
    df.loc[customer_idx, "country"] = new_country
    df.at[customer_idx, "past_addresses"] = past_addresses
    
    # Write back to parquet
    df.to_parquet(data_path, index=False)
    
    if logger:
        log_event(
            logger,
            "INFO",
            "customer_address_updated",
            customer_id=customer_id,
            old_address=old_address,
            new_city=new_city,
            new_country=new_country,
        )


if __name__ == "__main__":
    """Example usage of update_customer_address."""
    import sys
    from src.utils.logging_setup import get_logger
    
    if len(sys.argv) != 5:
        print("Usage: python -m src.utils.update_address <customer_id> <new_city> <new_country> <data_path>")
        print("Example: python -m src.utils.update_address CUST-001 'Los Angeles' US data/bronze/customers/data.parquet")
        sys.exit(1)
    
    customer_id = sys.argv[1]
    new_city = sys.argv[2]
    new_country = sys.argv[3]
    data_path = Path(sys.argv[4])
    
    logger = get_logger("update_address", Path("logs"))
    
    try:
        update_customer_address(customer_id, new_city, new_country, data_path, logger)
        print(f"Successfully updated address for {customer_id}")
        print(f"New address: {new_city} ({new_country})")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
