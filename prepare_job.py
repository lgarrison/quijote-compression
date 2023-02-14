#!/usr/bin/env python3

from pathlib import Path

import click

COMPRESS_HDF5 = (Path(__file__).parent / 'compress_hdf5.py').absolute()

@click.command()
@click.argument('root')
@click.argument('out')
def prepare(root, out):
    root = Path(root).absolute()
    out = Path(out).absolute()

    print(rf'#DISBATCH PREFIX {COMPRESS_HDF5} ')

    # for d in root.glob('*/'):
    #     for fn in d.glob('**/snap_*.hdf5'):
    #         print(f'{fn.absolute()} {out/fn.relative_to(root).parent}')
    #         break

    for fn in root.glob('**/snap_*.hdf5'):
        print(f'{fn.absolute()} {out/fn.relative_to(root).parent}')


if __name__ == '__main__':
    prepare()
