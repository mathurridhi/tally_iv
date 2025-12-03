"""Utility to flatten nested JSON structures into flat dictionaries"""
import pandas as pd
from typing import Any, Dict, List


def flatten_json(data: Any, parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """
    Recursively flatten a nested JSON structure.

    Args:
        data: The data to flatten (dict, list, or primitive)
        parent_key: The parent key for nested structures
        sep: Separator for concatenating keys

    Returns:
        Flattened dictionary with all nested fields
    """
    items = []

    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key

            if isinstance(value, dict):
                # Recursively flatten nested dictionaries
                items.extend(flatten_json(value, new_key, sep=sep).items())
            elif isinstance(value, list):
                # Handle lists
                if len(value) == 0:
                    items.append((new_key, None))
                elif all(isinstance(item, (str, int, float, bool, type(None))) for item in value):
                    # List of primitives - join them
                    items.append((new_key, ', '.join(str(v) for v in value if v is not None)))
                else:
                    # List of complex objects - flatten each with index
                    for idx, item in enumerate(value):
                        indexed_key = f"{new_key}_{idx}"
                        if isinstance(item, (dict, list)):
                            items.extend(flatten_json(item, indexed_key, sep=sep).items())
                        else:
                            items.append((indexed_key, item))
            else:
                # Primitive value
                items.append((new_key, value))

    elif isinstance(data, list):
        # Top-level list
        if len(data) == 0:
            items.append((parent_key or 'value', None))
        elif all(isinstance(item, (str, int, float, bool, type(None))) for item in data):
            # List of primitives
            items.append((parent_key or 'value', ', '.join(str(v) for v in data if v is not None)))
        else:
            # List of complex objects
            for idx, item in enumerate(data):
                indexed_key = f"{parent_key}_{idx}" if parent_key else str(idx)
                if isinstance(item, (dict, list)):
                    items.extend(flatten_json(item, indexed_key, sep=sep).items())
                else:
                    items.append((indexed_key, item))
    else:
        # Primitive value at top level
        items.append((parent_key or 'value', data))

    return dict(items)


def flatten_response_list(responses: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Flatten a list of API responses into a DataFrame.

    Args:
        responses: List of response dictionaries from API

    Returns:
        DataFrame with all fields flattened into columns
    """
    flattened_records = []

    for response in responses:
        flattened = {}

        # Add top-level fields
        flattened['status_code'] = response.get('status_code', '')
        flattened['success'] = response.get('success', False)

        # Flatten the response field if it exists
        if 'response' in response:
            response_data = response['response']
            if isinstance(response_data, dict):
                flattened_response = flatten_json(response_data)
                flattened.update(flattened_response)
            else:
                flattened['response'] = str(response_data)

        # Add error field if exists
        if 'error' in response:
            flattened['error'] = response.get('error', '')

        flattened_records.append(flattened)

    # Convert to DataFrame
    df = pd.DataFrame(flattened_records)

    return df
