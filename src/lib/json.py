from pathlib import Path
import json

class _SyncObject:
    def __init__(self, parent: '_SyncObject' = None):
        self._parent = parent

    def _translate(self, item: any) -> any:
        if isinstance(item, list) and not isinstance(item, _SyncList):
            return _SyncList(item, self)
        
        if isinstance(item, dict) and not isinstance(item, _SyncDict):
            return _SyncDict(item, self)
        
        return item

    def _sync(self):
        if not self._parent:
            return
        
        self._parent._sync()

class _SyncList(_SyncObject, list):
    def __init__(self, data: list, parent: '_SyncObject' = None):
        list.__init__(self, self._listTranslate(data))
        _SyncObject.__init__(self, parent)

    def _listTranslate(self, data: list) -> list:
        retVal = []
        for item in data:
            if isinstance(item, list) and not isinstance(item, _SyncList):
                retVal.append(_SyncList(item, self))
            elif isinstance(item, dict) and not isinstance(item, _SyncDict):
                retVal.append(_SyncDict(item, self))
            else:
                retVal.append(item)

        return retVal

    def __setattr__(self, name: str, value: any) -> None:
        list.__setattr__(self, name, self._translate(value))
        self._sync()

    def __setitem__(self, key: str, value: any):
        list.__setitem__(self, key, self._translate(value))
        self._sync()

    def append(self, object: any) -> None:
        super().append(self._translate(object))
        self._sync()

    def extend(self, object: any) -> None:
        super().extend(self._translate(object))
        self._sync()

class _SyncDict(_SyncObject, dict):
    def __init__(self, data: dict, parent: '_SyncObject' = None):
        dict.__init__(self, self._dictTranslate(data))
        _SyncObject.__init__(self, parent)

    def _dictTranslate(self, data: dict) -> dict:
        retVal = {}
        for key, value in data.items():
            if isinstance(value, list) and not isinstance(value, _SyncList):
                retVal[key] = _SyncList(value, self)
            elif isinstance(value, dict) and not isinstance(value, _SyncDict):
                retVal[key] = _SyncDict(value, self)
            else:
                retVal[key] = value

        return retVal

    def __setitem__(self, key: str, value: any):
        if key in dict(self) and dict(self).get(key) == value:
            return

        super().__setitem__(key, self._translate(value))
        self._sync()

    def __ior__(self, value: dict):
        value = super().__ior__(self._translate(value))
        self._sync()
        return value

    def clear(self):
        super().clear()
        super()._sync()

class JsonSynchroniser(_SyncDict):
    def __init__(self, filePath: Path):
        self._path = filePath

        if not filePath.exists():
            return super().__init__({})
        
        with open(filePath) as fp:
            super().__init__(json.load(fp))

    def _sync(self) -> None:
        with open(self._path, "w") as fp:
            json.dump(dict(self), fp, indent=4)
