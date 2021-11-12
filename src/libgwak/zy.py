#!/usr/bin/env python

import datetime
import re

class ZY:
    r: re = re.compile(r"^([\d]+)-([\d]+)-([\d]+)_([\d]+):([\d]+):([\d]+)$")

    def __init__(self, base: str = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwx', rx: str = '[0-9A-Za-x]'):
        self.z = tuple(base)
        self.b = {i:s for s,i in enumerate(base)}
        self.c = len(base)
        self.rx = rx

    def _enc(self, n: int) -> str:
        a = ''
        while True:
            a = self.z[n % self.c] + a
            n //= self.c
            if not n:
                return a

    def _dec(self, d: str) -> int:
        t = 0
        m = 1
        for n in range(len(s := tuple(d)) - 1, -1, -1):
            t += self.b[s[n]] * m
            m *= self.c
        return t

    def encode(self, time: datetime = datetime.datetime.utcnow()) -> str:
        return ''.join(map(self._enc,map(int,m.groups())))if(m:=self.r.match(time.strftime('%Y-%m-%d_%H:%M:%S')))else False

    def decode(self, zytime: str, oformat: str = '%04d-%02d-%02dT%02d:%02d:%02dZ', iformat: str = '(__)(_)(_)(_)(_)(_)') -> str:
        return oformat%tuple(map(self._dec,m.groups()))if(m:=re.match(fr"^z*{iformat}y*$".replace('_',self.rx),zytime))else False

zy = ZY()

def main():
    import argparse
    parser = argparse.ArgumentParser()
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument('-e', '--encode', type = datetime.datetime.fromisoformat, metavar = 'ISO_time')
    actions.add_argument('-d', '--decode', type = str, metavar = 'ZY_time')
    __params = parser.parse_args()

    if __params.encode:
        return zy.encode(__params.encode)
    if __params.decode:
        return zy.decode(__params.decode)
    return zy.encode()

if __name__ == '__main__':
    print(main())
