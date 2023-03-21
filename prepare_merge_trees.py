#!/usr/bin/env python3
'''
Prepare a disBatch script to copy files from one directory tree that do not exist in
another.  In this case, this include includes parameter files, log files, power
spectra, etc.
'''

import os
import re
from pathlib import Path
from timeit import default_timer

import click
from tqdm import tqdm

@click.command()
@click.argument('src')
@click.argument('dst')
@click.option('--verbose', '-V', is_flag=True, default=False)
def prepare(src, dst, verbose=False):
    t = -default_timer()
    src = Path(src).resolve()
    dst = Path(dst).resolve()

    i = 0
    for srcpath, dns, fns in tqdm(os.walk(src), unit=' path'):
        srcpath = Path(srcpath)
        dns[:] = [d for d in dns if not re.match(r'snapdir_\d+', d)]

        dstpath = dst / srcpath.relative_to(src)

        if dstpath.exists():
            copyfns = []
            for fn in fns:
                if re.match(r'ics\.\d+', fn):
                    continue
                if not (dstpath / fn).exists() or \
                    ((dstpath / fn).stat().st_size != (srcpath / fn).stat().st_size):
                    assert not fn.endswith('.hdf5')
                    copyfns += [str(srcpath / fn)]
            if copyfns:
                print(f'cp -t {str(dstpath)} {" ".join(copyfns)}')
        else:
            assert not re.match(r'snapdir_\d+', dstpath.name)
            print(f'cp -r {srcpath} {dstpath}')
            dns.clear()

        i += 1

    t += default_timer()
    if verbose:
        print(f'Time: {t:.4g} sec')
        print(f'Files: {i:.4g}')
        print(f'Rate: {i/t:.4g} files/sec')


if __name__ == '__main__':
    prepare()
