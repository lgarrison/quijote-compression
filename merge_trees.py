#!/usr/bin/env python3
'''
Copy any extra files from the tape staging area that were not compressed by
these scripts.  This includes parameter files, log files, power spectra, etc.
'''

import os
import re
import shutil
from pathlib import Path
from timeit import default_timer

import click
from tqdm import tqdm


@click.command()
@click.argument('src')
@click.argument('dst')
@click.option('--verbose', '-V', is_flag=True, default=False)
@click.option('--dryrun', '-d', is_flag=True, default=False)
def compress(src, dst, verbose=False, dryrun=False):
    t = -default_timer()
    src = Path(src).resolve()
    dst = Path(dst).resolve()

    if dryrun:
        # shutil.copy = lambda src,dst: print(f'Copy: {src} -> {dst}')
        # shutil.copytree = lambda src,dst: print(f'Copy tree: {src} -> {dst}')
        shutil.copy = lambda src,dst: print(src)
        shutil.copytree = lambda src,dst: print(src)

        chmod = lambda p,m: None
    else:
        chmod = lambda p,m: p.chmod(m)

    i = 0
    for srcpath, dns, fns in tqdm(os.walk(src), unit=' path'):
        srcpath = Path(srcpath)
        dns[:] = [d for d in dns if not re.match(r'snapdir_\d+', d)]

        dstpath = dst / srcpath.relative_to(src)

        if dstpath.exists():
            chmod(dstpath, 0o755)
            for fn in fns:
                if re.match(r'ics\.\d+', fn):
                    continue
                if not (dstpath / fn).exists() or \
                    ((dstpath / fn).stat().st_size != (srcpath / fn).stat().st_size):
                    assert not fn.endswith('.hdf5')
                    shutil.copy(srcpath / fn, dstpath / fn)
                    chmod(dstpath / fn, 0o444)
            chmod(dstpath, 0o555)
        else:
            assert not re.match(r'snapdir_\d+', dstpath.name)
            chmod(dstpath.parent, 0o755)
            shutil.copytree(srcpath, dstpath)
            chmod(dstpath, 0o555)
            chmod(dstpath.parent, 0o555)
            dns.clear()

        i += 1

    t += default_timer()
    if verbose:
        print(f'Time: {t:.4g} sec')
        print(f'Files: {i:.4g}')
        print(f'Rate: {i/t:.4g} files/sec')


if __name__ == '__main__':
    compress()
