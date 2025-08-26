from requests.auth import HTTPBasicAuth
import urllib.parse

class RepeatRequester:
    def __init__(self, baseURL: str):
        self.baseURL = baseURL

def buildAuth(username: str, password: str) -> HTTPBasicAuth:
    return HTTPBasicAuth(username, password)

def encodeParameters(parameters: dict) -> str:
    return urllib.parse.urlencode(parameters, quote_via=urllib.parse.quote)

