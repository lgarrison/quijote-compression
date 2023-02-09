#!/usr/bin/env python3

from itertools import islice
from pathlib import Path

import click


@click.command()
@click.argument('root')
@click.argument('out')
def prepare(root, out):
    root = Path(root)

    print(r'#DISBATCH PREFIX ../compress_hdf5.py ')

    for d in root.glob('*/'):
        for fn in d.glob('**/snap_*.hdf5'):
            print(f'{fn.absolute()} {out/fn.relative_to(root).parent}')
            break

    # for fn in root.glob('**/snap_*.hdf5'):
    #     print(f'{fn.absolute()} {out/fn.relative_to(root).parent}')


if __name__ == '__main__':
    prepare()
