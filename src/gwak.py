#!/usr/bin/env python

__version__ = "0.6.1"
import os
import shutil
import logging
from pathlib import Path
import libgwak.manifest
import libgwak.zy


def _filter_dir(path: str) -> Path:
    res = Path(path).resolve()
    if not res.is_dir():
        raise NotADirectoryError(path)
    return res


class ZYFormatter(logging.Formatter):
    def formatTime(self, *args, **kwargs) -> str:
        return libgwak.zy.zy.encode()


class Gwak:
    MANIFEST: str = 'gwak'
    GWAK: str = '._gwak'
    FILTERGLOB: str = '[!.]*'
    HASHALGO: str = 'sha3_512'

    minsize: int = 256
    mindupe: int = 2

    def __init__(self, params, **kwargs):
        self._params = params
        self._params.__dict__.update(kwargs)
        self._logger = self._params.logger
        if hasattr(self._params, 'minsize'):
            self.minsize = self._params.minsize
        if hasattr(self._params, 'mindupe'):
            self.mindupe = self._params.mindupe

    def _rmdir(self, path: Path) -> bool:
        if not path.is_dir() or any(path.iterdir()):
            return False
        self._logger.debug(f"rmdir [{path}]")
        if not self._params.dry_run:
            path.rmdir()
        return True

    def _is_smol(self, size: str) -> bool:
        return int(size, 16) < self.minsize

    def _bury(self, size: str, hash: str, file: Path) -> dict:
        body_dir = self._params.grave / size
        if not self._params.dry_run:
            body_dir.mkdir(parents = True, exist_ok = True)
        body = body_dir / hash
        link = body if self._params.isabs else os.path.relpath(body, file.parent)
        self._logger.debug(f"symlink [{link}]")
        if not self._params.dry_run:
            if body.is_file():
                file.unlink()
            else:
                file.rename(body)
            file.symlink_to(link)
        return {
            'link': str(link),
            'body': str(file)
        }

    def _exhume(self, file: Path, links: list) -> bool:
        self._logger.info(f"exhuming [{file}]")
        for link in links:
            self._logger.debug(f"copyfile [{link}]")
            if not self._params.dry_run:
                link.unlink()
                shutil.copyfile(file, link)
        if self._params.force and not self._params.dry_run:
            self._logger.debug(f"remove [{file}]")
            file.unlink()
        return True

    def _dedupe(self, gwaks: dict):
        for size, gwak in gwaks.items():
            if self._is_smol(size) and not self._params.force:
                self._logger.debug(f"skipping small files [{size}]")
                continue
            for hash, files in gwak.items():
                if len(files) < self.mindupe and not self._params.force:
                    self._logger.debug(f"skipping unique files [{hash}]")
                    continue
                for file in files:
                    yield self._bury(size, hash, file)

    def dedupe(self, gwaks: dict) -> list:
        return list(self._dedupe(gwaks))

    def _redupe(self, gwaks: dict):
        for size, gwak in gwaks.items():
            sizedir = self._params.grave / size
            for hash, links in gwak.items():
                file = sizedir / hash
                if not file.is_file():
                    self._logger.debug(f"skipping missing file [{file}]")
                    continue
                yield self._exhume(file, links)
            if self._params.force:
                self._rmdir(sizedir)

    def redupe(self, gwaks: dict) -> bool:
        return all(self._redupe(gwaks))

    def _validate_body(self, file: Path, size: str, hash: str) -> bool:
        if size != libgwak.manifest.gwak_size(file):
            self._logger.warning(f"size mismatch [{file}]")
            return False
        if hash != libgwak.manifest.gwak_hash(file, algo = self._params.hash):
            self._logger.warning(f"hash mismatch [{file}]")
            return False
        return True

    def _validate_files(self, gwaks: dict):
        for size, gwak in gwaks.items():
            for hash, files in gwak.items():
                for file in files:
                    if not file.is_file():
                        self._logger.warning(f"no such file [{file}]")
                        continue
                    yield self._validate_body(file, size, hash)

    def validate_files(self, gwaks: dict) -> bool:
        return all(self._validate_files(gwaks))

    def _validate_grave(self, gwaks: dict):
        for size, gwak in gwaks.items():
            for hash in gwak:
                file = self._params.grave / size / hash
                if not file.is_file():
                    self._logger.info(f"no such body [{file}]")
                    continue
                yield self._validate_body(file, size, hash)

    def validate_grave(self, gwaks: dict) -> bool:
        return all(self._validate_grave(gwaks))


def main():
    import sys
    import argparse
    import json
    import hashlib
    __LOGFORMAT = '%(asctime)s %(levelname)-8s | %(message)s'
    def run_gwak():
        global __params
        parser = argparse.ArgumentParser(description = "Gwak a directory by burying filebodies and replacing them with symlinks.")
        actions = parser.add_mutually_exclusive_group()
        parser.add_argument('-V', '--version', action = 'version', version = f"%(prog)s {__version__}")
        parser.add_argument('path', type = _filter_dir, nargs = '+', help = "target directory")
        parser.add_argument('-v', '--verbose', action = 'count', default = 0, help = "increase verbosity")
        parser.add_argument('-q', '--quiet', action = 'count', default = 0, help = "decrease verbosity")
        parser.add_argument('-m', '--manifest', type = Path, default = Gwak.MANIFEST, metavar = 'FILE', help = f"manifest file (default: {Gwak.MANIFEST})")
        parser.add_argument('--format', choices = libgwak.manifest.formats, default = libgwak.manifest.formats[0], help = "manifest format")
        parser.add_argument('--hash', choices = hashlib.algorithms_available, default = Gwak.HASHALGO, help = f"hash algo (default: {Gwak.HASHALGO})")
        parser.add_argument('-g', '--grave', type = Path, default = Gwak.GWAK, metavar = 'DIR', help = f"place to bury filebodies (default: {Gwak.GWAK} in first target directory)")
        parser.add_argument('-f', '--force', action = 'store_true', help = "gwak rare or small files, and delete filebodies")
        parser.add_argument('--exclude', type = str, nargs = '*', default = [], metavar = 'DIR', help = "exclude subdirectories by name")
        parser.add_argument('--filter', type = str, default = Gwak.FILTERGLOB, metavar = 'PATTERN', help = f"filter files and subdirectories by glob pattern (default: {Gwak.FILTERGLOB})")
        parser.add_argument('--minsize', type = int, default = Gwak.minsize, metavar = 'N', help = f"minimum file size to be replaced (default: {Gwak.minsize})")
        parser.add_argument('--mindupe', type = int, default = Gwak.mindupe, metavar = 'N', help = f"minimum file appearances to be replaced (default: {Gwak.mindupe})")
        parser.add_argument('--log', type = Path, metavar = 'FILE', help = "tee log to file")
        parser.add_argument('--dry-run', action = 'store_true', help = "do not write anything")
        actions.add_argument('-u', '--undo', '--ungwak', action = 'store_true', help = "ungwak by replacing symlinks with regular files")
        actions.add_argument('--validate', action = 'store_true', help = "validate gwaked directory")
        actions.add_argument('--check', action = 'store_true', help = "integrity check for filebodies")

        __params = parser.parse_args()
        __params.verbosity = __params.verbose - __params.quiet
        __params.isabs = __params.grave.is_absolute()
        if not __params.isabs:
            __params.exclude.append(__params.grave.name)
            __params.grave = __params.path[0] / __params.grave
        __params.manifest = __params.manifest if __params.manifest.is_absolute() else __params.grave / __params.manifest

        __params.logger = logging.getLogger('gwak')
        __params.logger.setLevel(logging.root.level - __params.verbosity * 10 - 1)
        if __params.log:
            __params.logger.addHandler(logf := logging.FileHandler(__params.log.resolve()))
            logf.setFormatter(ZYFormatter(__LOGFORMAT))
        __params.logger.addHandler(logh := logging.StreamHandler())
        logh.setFormatter(ZYFormatter(__LOGFORMAT))


        manifest = libgwak.manifest.Manifest(__params)
        gwak = Gwak(__params, gwakdata = manifest)

        match True:
            case __params.validate:
                return gwak.validate_files(manifest.load())
            case __params.check:
                return gwak.validate_grave(manifest.load())
            case __params.undo:
                return gwak.redupe(manifest.load())
            case _:
                manifest.make()
                manifest.write()
                return gwak.dedupe(manifest.get())

    result = run_gwak()
    if __params.verbosity >= 0:
        print(json.dumps(result, indent = 1))

    sys.exit(0 if result else 1)

if __name__ == '__main__':
    main()
