import json
from typing import Optional, Dict


def combine(result: Optional[str], kwargs: Dict) -> str:
    first = result if result else ''
    second = ", ".join(f"{k} = {json.dumps(v)}" for k, v in kwargs.items()) if kwargs else ''
    args = ', '.join(element for element in (first, second) if element)
    return args
