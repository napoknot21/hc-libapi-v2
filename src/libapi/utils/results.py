from __future__ import annotations

import os
import re
import json

from typing import Optional, Dict

from libapi.config.parameters import LIBAPI_CACHE_RESULTS_DIR_PATH


def find_cache_results_from_id (
        
        calculation_id : Optional[int | str] = None,
        dir_abs_path : Optional[str] = None
    
    ) -> Optional[str] :
    """
    
    """
    if calculation_id is None :

        print(f"\n[-] No Calculation ID. Using the API...")
        return None
    
    dir_abs_path = LIBAPI_CACHE_RESULTS_DIR_PATH if dir_abs_path is None else dir_abs_path
    
    if not os.path.exists(dir_abs_path) :

        os.makedirs(dir_abs_path, exist_ok=True)
        return None
    
    regex = re.compile(rf"^{re.escape(str(calculation_id))}_results\.json$")

    for entry in os.listdir(dir_abs_path) :

        m = regex.match(entry)

        if not m :
            continue

        filename_abs_path = os.path.join(dir_abs_path, entry)

        return filename_abs_path
        
    return None


def load_cache_results_from_id (
        
        calculation_id : Optional[int | str] = None,
        dir_abs_path : Optional[str] = None
    
    ) -> Optional[Dict] :
    """
    
    """
    filename_abs_path = find_cache_results_from_id(calculation_id, dir_abs_path)

    if filename_abs_path is None :

        return None
    
    with open(filename_abs_path, "r", encoding="utf-8") as f :
        
        data = json.load(f)

    return data

    
def save_cache_results (
        
        calculation_id : Optional[str | int] = None,
        data : Optional[Dict] = None,
        dir_abs_path : Optional[str] = None
    
    ) -> bool :
    """
    
    """
    dir_abs_path = LIBAPI_CACHE_RESULTS_DIR_PATH if dir_abs_path is None else dir_abs_path

    filename_path = find_cache_results_from_id(calculation_id, dir_abs_path)

    # In this case, the file already exists
    if filename_path is not None :

        print(f"[-] A file already exists for the ID : {calculation_id}")
        return False
    
    filename = f"{calculation_id}_results.json"
    full_path = os.path.join(dir_abs_path, filename)

    with open(full_path, "w", encoding="utf-8") as f :
        json.dump(data, f, indent=4, ensure_ascii=False)

    return True