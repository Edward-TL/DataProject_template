import requests
import json

def simple_data(url, headers: dict = None) -> dict:
    """
    Sugar sintax for getting data from api.
    Remember to format URL before using it.
    """
    if headers is None:
        response = requests.get(url)
    else:
        response = requests.get(url, headers)
    
    data = json.loads(response.text)

    return data 

