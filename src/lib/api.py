import requests
from enum import Enum

class DataType(Enum):
    OBJECT  = 0
    CONTENT = 1
    TEXT    = 2
    JSON    = 3
    HEADERS = 4

class RepeatRequester:
    def __init__(self, headers: dict = {}, defaultDatatype: DataType = DataType.CONTENT):
        self.headers = headers
        self.responseDataType = defaultDatatype

        self.session = None

    def getResponse(self, url: str, headers: dict = {}) -> str | dict | requests.Response:
        if self.session is None:
            self.session = requests.Session()

        response = self.session.get(url, headers=self.headers | headers)
        return self.parseResponse(response)

    def parseResponse(self, response: requests.Response):
        if self.responseDataType == DataType.CONTENT:
            return response.content
        
        if self.responseDataType == DataType.TEXT:
            return response.text
        
        if self.responseDataType == DataType.JSON:
            return response.json()
        
        if self.responseDataType == DataType.HEADERS:
            return response.headers

        return response

def addURLParameters(url: str, parameters: dict) -> str:
    return f"{url}{'&'.join(f'{key}={value}' for key, value in parameters.items())}"
