#!/usr/bin/env python3

from pathlib import Path

import click


@click.command()
@click.argument('simdir')
def prepare(simdir):
    simdir = Path(simdir)

    print(
'''#DISBATCH PREFIX cd $HOME/scc/fvillaescusa-compression; ./compress_hdf5.py 
#DISBATCH SUFFIX  -p 6 -v 11
''')

    for snapdir in simdir.glob('snapdir_*/'):
        for fn in snapdir.glob('snap_*.hdf5'):
            print(f'{fn} out/{snapdir.name}/')


if __name__ == '__main__':
    prepare()
