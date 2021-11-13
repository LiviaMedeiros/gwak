#!/usr/bin/env python

__version__ = "0.4.0"
import sys
import os
import shutil
import logging
from pathlib import Path
import json
import libgwak.manifest
import libgwak.zy


__MINSIZE = 256
__MINDUPE = 2
__MANIFEST = 'gwak'
__GWAK = '._gwak'
__FILTERGLOB = '[!.]*'
__LOGFORMAT = '%(asctime)s %(levelname)-8s | %(message)s'

__params = None


class ZYFormatter(logging.Formatter):
    def formatTime(self, *args, **kwargs) -> str:
        return libgwak.zy.zy.encode()


def _filter_dir(path: str) -> Path:
    res = Path(path).resolve()
    if not res.is_dir():
        raise NotADirectoryError(path)
    return res

def _rmdir(path: Path) -> bool:
    if not path.is_dir() or any(path.iterdir()):
        return False
    __params.logger.debug(f"rmdir [{path}]")
    if not __params.dry_run:
        path.rmdir()
    return True

def _is_smol(size: str) -> bool:
    return int(size, 16) < __params.minsize

def _bury(size: str, hash: str, file: Path) -> dict:
    body_dir = __params.grave / size
    if not __params.dry_run:
        body_dir.mkdir(parents = True, exist_ok = True)
    body = body_dir / hash
    link = body if __params.isabs else os.path.relpath(body, file.parent)
    __params.logger.debug(f"symlink [{link}]")
    if not __params.dry_run:
        if body.is_file():
            file.unlink()
        else:
            file.rename(body)
        file.symlink_to(link)
    return {
        'link': str(link),
        'body': str(file)
    }

def _exhume(file: Path, links: list) -> bool:
    __params.logger.info(f"exhuming [{file}]")
    for link in links:
        __params.logger.debug(f"copyfile [{link}]")
        if not __params.dry_run:
            link.unlink()
            shutil.copyfile(file, link)
    if __params.force and not __params.dry_run:
        __params.logger.debug(f"remove [{file}]")
        file.unlink()
    return True

def _dedupe(gwaks: dict):
    for size, gwak in gwaks.items():
        if _is_smol(size) and not __params.force:
            __params.logger.debug(f"skipping small files [{size}]")
            continue
        for hash, files in gwak.items():
            if len(files) < __params.mindupe and not __params.force:
                __params.logger.debug(f"skipping unique files [{hash}]")
                continue
            for file in files:
                yield _bury(size, hash, file)

def dedupe(gwaks: dict) -> list:
    return list(_dedupe(gwaks))

def _redupe(gwaks: dict, grave: Path):
    for size, gwak in gwaks.items():
        sizedir = grave / size
        for hash, links in gwak.items():
            hashdir = sizedir / hash
            file = sizedir / hash
            if not file.is_file():
                __params.logger.debug(f"skipping missing file [{file}]")
                continue
            yield _exhume(file, links)
            if __params.force:
                _rmdir(hashdir)
        if __params.force:
            _rmdir(sizedir)

def redupe(gwaks: dict, grave: str) -> bool:
    return all(_redupe(gwaks, grave))

def _validate_body(file: Path, size: str, hash: str) -> bool:
    body = file.read_bytes()
    if size != libgwak.manifest.gwak_size(body):
        __params.logger.warning(f"size mismatch [{file}]")
        return False
    if hash != libgwak.manifest.gwak_hash(body):
        __params.logger.warning(f"hash mismatch [{file}]")
        return False
    return True

def _validate_files(gwaks: dict):
    for size, gwak in gwaks.items():
        for hash, files in gwak.items():
            for file in files:
                if not file.is_file():
                    __params.logger.warning("no such file [{file}]")
                    continue
                yield _validate_body(file, size, hash)

def validate_files(gwaks: dict) -> bool:
    return all(_validate_files(gwaks))

def _validate_grave(gwaks: dict, grave: Path):
    for size, gwak in gwaks.items():
        for hash in gwak:
            file = grave / size / hash
            if not file.is_file():
                __params.logger.info(f"no such body [{file}]")
                continue
            yield _validate_body(file, size, hash)

def validate_grave(gwaks: dict, grave: Path) -> bool:
    return all(_validate_grave(gwaks, grave))


def main():
    import argparse
    def run_gwak():
        global __params
        parser = argparse.ArgumentParser(description = "Gwak a directory by burying filebodies and replacing them with symlinks.")
        actions = parser.add_mutually_exclusive_group()
        parser.add_argument('-V', '--version', action = 'version', version = f"%(prog)s {__version__}")
        parser.add_argument('path', type = _filter_dir, nargs = '+', help = "target directory")
        parser.add_argument('-v', '--verbose', action = 'count', default = 0, help = "increase verbosity")
        parser.add_argument('-q', '--quiet', action = 'count', default = 0, help = "decrease verbosity")
        parser.add_argument('-m', '--manifest', type = Path, default = __MANIFEST, metavar = 'FILE', help = f"manifest file (default: {__MANIFEST})")
        parser.add_argument('--format', choices = libgwak.manifest.formats, default = libgwak.manifest.formats[0], help = "manifest format")
        parser.add_argument('-g', '--grave', type = Path, default = __GWAK, metavar = 'DIR', help = f"place to bury filebodies (default: {__GWAK} in first target directory)")
        parser.add_argument('-f', '--force', action = 'store_true', help = "gwak rare or small files, and delete filebodies")
        parser.add_argument('--exclude', type = str, nargs = '*', default = [], metavar = 'DIR', help = "exclude subdirectories by name")
        parser.add_argument('--filter', type = str, default = __FILTERGLOB, metavar = 'PATTERN', help = f"filter files and subdirectories by glob pattern (default: {__FILTERGLOB})")
        parser.add_argument('--minsize', type = int, default = __MINSIZE, metavar = 'N', help = f"minimum file size to be replaced (default: {__MINSIZE})")
        parser.add_argument('--mindupe', type = int, default = __MINDUPE, metavar = 'N', help = f"minimum file appearances to be replaced (default: {__MINDUPE})")
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


        gwaks = libgwak.manifest.Manifest(__params)

        if __params.validate:
            return validate_files(gwaks.load())
        if __params.check:
            return validate_grave(gwaks.load(), __params.grave)
        if __params.undo:
            return redupe(gwaks.load(), __params.grave)

        gwaks.make()
        gwaks.write()
        return dedupe(gwaks.get())

    result = run_gwak()
    if __params.verbosity >= 0:
        print(json.dumps(result, indent = 1))

    sys.exit(0 if result else 1)

if __name__ == '__main__':
    main()
