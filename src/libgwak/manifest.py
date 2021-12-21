import collections
import hashlib
from pathlib import Path

formats = ['yaml', 'json', 'csv']
try:
    import csv
except ImportError:
    formats.remove('csv')
try:
    import json
except ImportError:
    formats.remove('json')
try:
    import yaml
    #yaml.add_representer(collections.defaultdict, yaml.representer.Representer.represent_dict)
except ImportError:
    formats.remove('yaml')


def gwak_size(file: Path) -> str:
    return f"{file.stat().st_size:016x}"

def gwak_hash(file: Path, algo: str = 'sha3_512', blocksize: int = 0x100000) -> str:
    hash = hashlib.new(algo)
    with file.open(mode = 'rb') as f:
        while chunk := f.read(blocksize):
            hash.update(chunk)
    return hash.hexdigest()


class Manifest():
    _data: dict = {}
    _file: Path = None
    _logger = None
    _params: dict = {}


    def __init__(self, params: dict):
        self._params = params
        self._logger = params.logger
        self._file = params.manifest

    def _normalize(self, data: dict) -> dict:
        return {k: dict(v) for k, v in data.items()}

    def _transform(self, data: dict, mutator: callable = str) -> dict:
        return {size: {hash: [mutator(file)
            for file in files]
            for hash, files in gwak.items()}
            for size, gwak in data.items()}

    def _walk(self, path: Path):
        for item in path.iterdir():
            if item.name in self._params.exclude:
                self._logger.info(f"excluding path [{item}]")
                continue
            if not item.match(self._params.filter):
                self._logger.info(f"filtering out path [{item}]")
                continue
            if item.is_symlink():
                self._logger.debug(f"skipping symlink [{item}]")
                continue
            if item.is_dir():
                yield from self._walk(item)
                continue
            if not item.is_file():
                self._logger.debug(f"skipping irregular file [{item}]")
                continue
            yield item.resolve()

    def _gen_bytree(self):
        for path in self._params.path:
            for file in self._walk(path):
                yield gwak_size(file), gwak_hash(file, self._params.hash), file

    def _gen_bytable(self, files):
        for size, hash, file in files:
            yield size, hash, Path(file)

    def _csvload(self, f) -> dict:
        return self._makedict(csv.reader(f))

    def _makedict(self, gen) -> dict:
        gwaks = collections.defaultdict(lambda: collections.defaultdict(list))
        for size, hash, file in gen:
            gwaks[size][hash].append(file)
        return self._normalize(gwaks)

    def _backup(self) -> None:
        self._logger.warning(f"backing up old manifest [{self._file}]")
        hash = hashlib.sha3_512(self._file.read_bytes()).hexdigest()
        return self._file.rename(f"{self._file}.{hash}")

    def _read(self) -> dict:
        with self._file.open(mode = 'r') as f:
            match self._params.format:
                case 'yaml':
                    return yaml.safe_load(f)
                case 'json':
                    return json.load(f)
                case 'csv':
                    return self._csvload(f)

    def _write(self) -> None:
        with self._file.open(mode = 'w') as f:
            match self._params.format:
                case 'yaml':
                    return yaml.dump(self.serialize(), f)
                case 'json':
                    return json.dump(self.serialize(), f)
                case 'csv':
                    return csv.writer(f).writerows(self.pettan())


    def get(self) -> dict:
        return self._data

    def read(self) -> dict:
        self._logger.info(f"reading manifest [{self._file}]")
        if self._params.format not in formats:
            raise NotImplementedError(self._params.format)
        return self._read()

    def load(self, mutator: callable = Path) -> dict:
        self._data = self._transform(self.read(), mutator)
        return self._data

    def write(self) -> None | bool:
        self._logger.info(f"writing manifest [{self._file}]")
        if self._params.dry_run:
            return True
        self._file.parent.mkdir(parents = True, exist_ok = True)
        if self._file.is_file():
            self._backup()
        if self._params.format not in formats:
            raise NotImplementedError(self._params.format)
        return self._write()

    def make(self) -> dict:
        self._data = self._makedict(self._gen_bytree())
        return self._data

    def pettan(self, m: callable = str):
        return ((s,h,m(p))for s,g in self._data.items()for h,f in g.items()for p in f)

    def serialize(self, m: callable = str) -> dict:
        return self._transform(self._data, m)
