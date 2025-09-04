import requests
from requests.auth import HTTPBasicAuth
import urllib.parse

class RepeatRequester:
    def __init__(self, baseURL: str, headers: dict):
        self.baseURL = baseURL
        self.headers = headers

        self.session: requests.Session | None = None

    def _setsession(self, func: callable):
        def wrapped(*args, **kwargs):
            if self.session is None:
                self.session = requests.Session()

            return func(*args, **kwargs)

        return wrapped

    @_setsession
    def get(self, endpoint: str, params: dict = {}, headers: dict = {}, **kwargs: dict) -> requests.Response:
        return self.session.get(self.baseURL, endpoint=endpoint, params=encodeParameters(params), headers=self.headers | headers, **kwargs)

    @_setsession
    def post(self, endpoint: str, params: dict = {}, headers: dict = {}, **kwargs: dict) -> requests.Response:
        return self.session.post(self.baseURL, endpoint=endpoint, params=encodeParameters(params), headers=self.headers | headers, **kwargs)

def buildAuth(username: str, password: str) -> HTTPBasicAuth | None:
    if not username:
        return
    
    return HTTPBasicAuth(username, password)

def encodeParameters(parameters: dict) -> str:
    return urllib.parse.urlencode(parameters, quote_via=urllib.parse.quote)
