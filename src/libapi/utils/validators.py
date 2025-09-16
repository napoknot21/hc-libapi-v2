from __future__ import annotations

from typing import List


def validate_flat_strikes (strikes : List) :
    """
    
    """
    if not strikes:
        return
    
    if not isinstance(strikes[0], (tuple, list)) :
        raise ValueError("`strikes` must be a flat list of single values, not tuples/lists.")
    

def validate_direction (direction : str) -> None :
    """
    
    """
    if direction not in {"Buy", "Sell"} :
        raise ValueError('`direction` must be "Buy" or "Sell".')