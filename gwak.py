#!/usr/bin/env python
import sys
import os
import shutil
import collections
import hashlib
import logging

__formats = ['yaml', 'json']
try:
    import json
except ImportError:
    __formats.remove('json')
try:
    import yaml
    #yaml.add_representer(collections.defaultdict, yaml.representer.Representer.represent_dict)
except ImportError:
    __formats.remove('yaml')


__MINSIZE = 256
__MINDUPE = 2
__MANIFEST = "gwak"
__GWAK = "._gwak"

__params = None


def _filter_dir(path: str) -> str:
    if not os.path.isdir(path):
        raise NotADirectoryError(string)
    return os.path.join(path, '')

def _is_not_regular_file(path: str) -> bool:
    return not os.path.isfile(path) or os.path.islink(path)

def _is_smol(size: str) -> bool:
    return int(size, 16) < __params.minsize

def __format_size(body: bytes) -> str:
    return '{:016x}'.format(len(body))

def __format_hash(body: bytes) -> str:
    return hashlib.sha3_512(body).hexdigest()

def _bury(size: str, hash: str, file: str) -> tuple:
    body_dirname = os.path.join(__params.graveyard, size)
    if not __params.dry_run:
        os.makedirs(body_dirname, exist_ok = True)
    body = os.path.join(body_dirname, hash)
    link = (body if __params.isabs else os.path.relpath(body, os.path.dirname(file)), file)
    logging.debug(f"symlink [{link}]")
    if not __params.dry_run:
        if os.path.isfile(body):
            os.remove(file)
        else:
            os.rename(file, body)
        os.symlink(*link)
    return link

def _exhume(file: str, links: list) -> bool:
    logging.info(f"exhuming [{file}]")
    for link in links:
        logging.debug(f"copyfile [{link}]")
        if not __params.dry_run:
            os.remove(link)
            shutil.copyfile(file, link)
    if __params.force and not __params.dry_run:
        logging.debug(f"remove [{file}]")
        os.remove(file)
    return True

def _write_manifest(data: dict) -> None | bool:
    logging.info(f"writing manifest [{__params.manifest}]")
    if __params.dry_run:
        return True
    os.makedirs(os.path.dirname(__params.manifest), exist_ok = True)
    if os.path.isfile(__params.manifest):
        _backup_manifest()
    if __params.format not in __formats:
        raise NotImplementedError(__params.format)
    with open(__params.manifest, 'w') as f:
        match __params.format:
            case 'yaml':
                return yaml.dump(data, f)
            case 'json':
                return json.dump(data, f)

def _read_manifest() -> dict:
    logging.info(f"reading manifest [{__params.manifest}]")
    if __params.format not in __formats:
        raise NotImplementedError(__params.format)
    with open(__params.manifest, 'r') as f:
        match __params.format:
            case 'yaml':
                return yaml.safe_load(f)
            case 'json':
                return json.load(f)

def _backup_manifest() -> None:
    logging.warning(f"backing up old manifest [{__params.manifest}]")
    with open(__params.manifest, 'rb') as f:
        hash = hashlib.sha3_512(f.read()).hexdigest()
    return os.rename(__params.manifest, f"{__params.manifest}.{hash}")

def make_manifest(paths: list) -> dict:
    gwaks = collections.defaultdict(lambda: collections.defaultdict(list))
    for path in paths:
        for root, dirs, files in os.walk(path):
            if __params.grave in dirs:
                dirs.remove(__params.grave)
            for file in files:
                file = os.path.join(root, file)
                if _is_not_regular_file(file):
                    logging.info(f"skipping irregular file [{file}]")
                    continue
                with open(file, 'rb') as f:
                    body = f.read()
                gwaks[__format_size(body)][__format_hash(body)].append(file)
    return {k: dict(v) for k, v in gwaks.items()}

def _dedupe(gwaks: dict):
    for size, gwak in gwaks.items():
        if _is_smol(size) and not __params.force:
            logging.debug(f"skipping small files [{size}]")
            continue
        for hash, files in gwak.items():
            if len(files) < __params.mindupe and not __params.force:
                logging.debug(f"skipping unique files [{hash}]")
                continue
            for file in files:
                yield _bury(size, hash, file)

def dedupe(gwaks: dict) -> list:
    return list(_dedupe(gwaks))

def _redupe(gwaks: dict, grave: str):
    for size, gwak in gwaks.items():
        for hash, links in gwak.items():
            file = os.path.join(grave, size, hash)
            if not os.path.isfile(file):
                logging.debug(f"skipping missing file [{file}]")
                continue
            yield _exhume(file, links)

def redupe(gwaks: dict, grave: str) -> bool:
    return all(_redupe(gwaks, grave))

def _validate_body(file: str, size: str, hash: str) -> bool:
    with open(file, 'rb') as f:
        body = f.read()
    if size != __format_size(body):
        logging.warning(f"size mismatch [{file}]")
        return False
    if hash != __format_hash(body):
        logging.warning(f"hash mismatch [{file}]")
        return False
    return True

def _validate_files(gwaks: dict):
    for size, gwak in gwaks.items():
        for hash, files in gwak.items():
            for file in files:
                if not os.path.isfile(file):
                    logging.warning("no such file [{file}]")
                    continue
                yield _validate_body(file, size, hash)

def validate_files(gwaks: dict) -> bool:
    return all(_validate_files(gwaks))

def _validate_grave(gwaks: dict, grave: str):
    for size, gwak in gwaks.items():
        for hash in gwak:
            file = os.path.join(grave, size, hash)
            if not os.path.isfile(file):
                logging.info(f"no such body [{file}]")
                continue
            yield _validate_body(file, size, hash)

def validate_grave(gwaks: dict, grave: str) -> bool:
    return all(_validate_grave(gwaks, grave))


if __name__ == '__main__':
    import argparse
    def run_gwak():
        global __params
        parser = argparse.ArgumentParser(description = "Gwak a directory by burying filebodies and replacing them with symlinks.")
        parser.add_argument('path', type = _filter_dir, nargs = '+', help = "target directory")
        parser.add_argument('-v', '--verbose', action = 'count', default = 0, help = "increase verbosity")
        parser.add_argument('-q', '--quiet', action = 'count', default = 0, help = "decrease verbosity")
        parser.add_argument('-m', '--manifest', type = str, default = __MANIFEST, metavar = "FILEPATH", help = f"manifest file (default: {__MANIFEST})")
        parser.add_argument('--format', choices = __formats, default = __formats[0], help = "manifest format")
        parser.add_argument('-g', '--grave', type = str, default = __GWAK, metavar = "DIR", help = f"place to bury filebodies (default: {__GWAK} in first target directory)")
        parser.add_argument('-f', '--force', action = 'store_true', help = "gwak rare or small files, and delete filebodies")
        parser.add_argument('-u', '--undo', '--ungwak', action = 'store_true', help = "ungwak by replacing symlinks with regular files")
        parser.add_argument('--minsize', type = int, default = __MINSIZE, metavar = "N", help = f"minimum file size to be replaced (default: {__MINSIZE})")
        parser.add_argument('--mindupe', type = int, default = __MINDUPE, metavar = "N", help = f"minimum file appearances to be replaced (default: {__MINDUPE})")
        parser.add_argument('--validate', action = 'store_true', help = "validate gwaked directory")
        parser.add_argument('--check', action = 'store_true', help = "integrity check for filebodies")
        parser.add_argument('--dry-run', action = 'store_true', help = "do not write anything")

        __params = parser.parse_args()
        __params.verbosity = __params.verbose - __params.quiet
        __params.isabs = os.path.isabs(__params.grave)
        __params.graveyard = os.path.join(__params.grave, '') if __params.isabs else os.path.join(__params.path[0], __params.grave)
        __params.manifest = __params.manifest if os.path.isabs(__params.manifest) else os.path.join(__params.graveyard, __params.manifest)

        logging.root.setLevel(logging.root.level - __params.verbosity * 10)


        if __params.validate:
            return validate_files(_read_manifest())
        if __params.check:
            return validate_grave(_read_manifest(), __params.graveyard)
        if __params.undo:
            return redupe(_read_manifest(), __params.graveyard)

        gwaks = make_manifest(__params.path)
        _write_manifest(gwaks)
        return dedupe(gwaks)

    result = run_gwak()
    if __params.verbosity >= 0:
        print(json.dumps(result, indent = 1))

    sys.exit(0 if result else 1)
