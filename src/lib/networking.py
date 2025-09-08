import requests
from requests.auth import HTTPBasicAuth
import urllib.parse
from enum import Enum

class RequestType(Enum):
    HEAD = "HEAD"
    GET  = "GET"
    POST = "POST"

class RepeatRequester:
    def __init__(self, baseURL: str, headers: dict, params: dict, username: str = "", password: str = ""):
        self.baseURL = baseURL
        self.headers = headers
        self.params = params
        self.auth = buildAuth(username, password)

        self.session: requests.Session | None = None

    def request(self, endpoint: str, method: RequestType = RequestType.GET, **kwargs: dict) -> requests.Response:
        if self.session is None:
            self.session = requests.session()

        return self.session.request(
            method.value,
            urllib.parse.urljoin(self.baseURL, endpoint),
            headers=self.headers | kwargs.pop("headers", {}),
            params=encodeParameters(self.params | kwargs.pop("params", {})),
            **kwargs
        )
    
    def get(self, endpoint: str, **kwargs: dict) -> requests.Response:
        return self.request(endpoint, RequestType.GET, **kwargs)

    def post(self, endpoint: str, **kwargs: dict) -> requests.Response:
        return self.request(endpoint, RequestType.POST, **kwargs)
    
    def head(self, endpoint: str, **kwargs: dict) -> requests.Response:
        return self.request(endpoint, RequestType.HEAD, **kwargs)

def buildAuth(username: str, password: str) -> HTTPBasicAuth | None:
    if not username:
        return
    
    return HTTPBasicAuth(username, password)

def encodeParameters(parameters: dict) -> str:
    return urllib.parse.urlencode(parameters, quote_via=urllib.parse.quote)
