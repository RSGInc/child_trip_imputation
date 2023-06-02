from utils import settings

def get_codes(code_name: str):
    """
    Asserts and returns the key-values pair for the requested codebook code
    
    Args:
        code_name (str): name of code pair under CODES in settings.yaml

    Returns:
        tuple: (column name: str, codes: list)
    """
    assert isinstance(settings.CODES, dict)        
    code = settings.CODES.get(code_name)
    
    assert isinstance(code, dict), f'{code_name} not in CODES'
    
    col, codes = [(k, list(v)) for k, v in code.items()][0]
    
    return (col, codes)