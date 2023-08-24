import requests
import json

APPLICATION_JSON = {"Content-Type": "application/json"}

def GET(url: str, **kwargs) -> requests.Response: # type: ignore
    """
    Make a GET request against the passed URL.

    :param url: The url endpoint for the request
    :param **kwargs: Other parameters available to the requests.request() method

    :returns a request Response:
    """
    response = requests.request(method="GET", url=url, **kwargs)

    if response.ok:
        return response
    else:
        response.raise_for_status()

def GET_DICT(url: str, **kwargs) -> dict:
    """
    Make a GET request against the passed URL, but return a dictionary of the json text response.

    :param url: The url endpoint for the request
    :param **kwargs: Other parameters available to the requests.request() method

    :returns a dictionary of the request Response's text:
    """
    response = GET(url, **kwargs)
    response_dict = json.loads(response.text)
    return response_dict

def POST( url: str, **kwargs) -> requests.Response: # type: ignore
    """
    Make a POST request against the passed URL.

    :param url: The url endpoint for the request
    :param **kwargs: Other parameters available to the requests.request() method

    :returns a request Response:
    """
    response = requests.request(method="POST", url=url, **kwargs)

    if response.ok:
        return response
    else:
        response.raise_for_status()

def PUT( url: str, **kwargs) -> requests.Response: # type: ignore
    """
    Make a PUT request against the passed URL

    :param url: The url endpoint for the request
    :param **kwargs: Other parameters available to the requests.request() method

    :returns a request Response:
    """
    response = requests.request(method="PUT", url=url, **kwargs)

    if response.ok:
        return response
    else:
        response.raise_for_status()

def PATCH( url: str, **kwargs) -> requests.Response: # type: ignore
    """
    Make a PATCH request against the passed URL

    :param url: The url endpoint for the request
    :param **kwargs: Other parameters to pass to the request, like json

    :returns a request Response:
    """
    response = requests.request(method="PATCH", url=url, **kwargs)

    if response.ok:
        return response
    else:
        response.raise_for_status()

