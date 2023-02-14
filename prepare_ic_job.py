#!/usr/bin/env python3

from pathlib import Path

import click

COMPRESS_SCRIPT = (Path(__file__).parent / 'compress_IC.py').absolute()

NOUT = {128: 8,
        512: 8,
        }

@click.command()
@click.argument('root')
@click.argument('out')
def prepare(root, out):
    root = Path(root).absolute()
    out = Path(out).absolute()

    print(rf'#DISBATCH PREFIX {COMPRESS_SCRIPT} ')

    for d in root.glob('*/*/ICs'):
        fns = sorted(d.glob('ics.*'), key=lambda f: int(f.suffix[1:]))
        
        nout = NOUT[len(fns)]
        ncat = len(fns)//nout
        chunks = [fns[ncat*i:ncat*(i+1)] for i in range(nout)]
        assert sum(len(c) for c in chunks) == len(fns)
        for i,c in enumerate(chunks):
            print(" ".join(str(f) for f in c) + " " + \
                str(out/d.relative_to(root)/f"ics.{i}.hdf5")
                )


if __name__ == '__main__':
    prepare()
