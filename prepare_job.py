#!/usr/bin/env python3

from pathlib import Path
import sys

import click

COMPRESS_HDF5 = (Path(__file__).parent / 'compress_hdf5.py').resolve()
COMPRESS_GADGET = (Path(__file__).parent / 'compress_gadget.py').resolve()

NOUT_IC = { 64: 8,
           128: 8,
           512: 8,
           }

@click.command()
@click.argument('root')
@click.argument('out')
def main(root, out):
    prepare(root, out)
    prepare_ic(root, out)


def prepare(root, out):
    root = Path(root).resolve()
    out = Path(out).resolve()

    hdf5_tasks = []
    gadget_tasks = []
    for fn in root.glob('**/snap_*.*'):
        assert 'snapdir_' in fn.parent.name
        if fn.suffix == '.hdf5':
            hdf5_tasks += [f'{fn} {out/fn.relative_to(root).parent}']
        else:
            outfn = out/fn.relative_to(root)
            outfn = outfn.parent / (outfn.name + '.hdf5')
            gadget_tasks += [f'{fn} {outfn}']

    print(rf'#DISBATCH PREFIX {COMPRESS_HDF5} ')
    for task in hdf5_tasks:
        print(task)

    print(rf'#DISBATCH PREFIX {COMPRESS_GADGET} -s ')
    for task in gadget_tasks:
        print(task)


def prepare_ic(root, out):
    root = Path(root).resolve()
    out = Path(out).resolve()

    print(rf'#DISBATCH PREFIX {COMPRESS_GADGET} -v 0 -p 0 ')

    for d in root.glob('*/*/ICs'):
        fns = sorted(d.glob('ics.*'), key=lambda f: int(f.suffix[1:]))

        if len(fns) == 0:
            continue
        
        try:
            nout = NOUT_IC[len(fns)]
        except KeyError as k:
            print(d, file=sys.stderr)
            raise k
        
        ncat = len(fns)//nout
        chunks = [fns[ncat*i:ncat*(i+1)] for i in range(nout)]
        assert sum(len(c) for c in chunks) == len(fns)
        for i,c in enumerate(chunks):
            print(" ".join(str(f) for f in c) + " " + \
                str(out/d.relative_to(root)/f"ics.{i}.hdf5")
                )


if __name__ == '__main__':
    main()
