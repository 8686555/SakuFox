import math
from typing import Any

def sanitize_for_json(obj: Any) -> Any:
    """
    Recursively replace NaN and Inf values with None (which translates to null in JSON).
    Handles dicts, lists, tuples, and nested structures.
    """
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [sanitize_for_json(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    # Handle numpy scalars if numpy is present
    try:
        import numpy as np
        if isinstance(obj, np.generic):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return obj.item()
    except ImportError:
        pass
        
    return obj
