#!/usr/bin/env python3

from pathlib import Path

import asdf
import click
import h5py
import numpy as np


@click.command()
@click.argument('src', nargs=-1)
@click.argument('dst')
@click.option('--truncpos', '-p', default=0,
    help='Number of low bits to null out in the position data',
)
@click.option('--truncvel', '-v', default=0,
    help='Number of low bits to null out in the velocity data',
)
def compress(src, dst, truncpos=0, truncvel=0,):
    dst = Path(dst)

    compression_opts = dict(
        Coordinates=dict(
            asdf=dict(
                compression='blsc',
                compression_block_size=12*(1<<20),
                blosc_block_size=3*(1<<20),
                cname='zstd',
                clevel=1,
                shuffle='bitshuffle',
                typesize=4,
            ),
            truncbits=truncpos,
        ),
        # Velocities=dict(
        #     asdf=dict(
        #         compression='blsc',
        #         compression_block_size=12*(1<<20),
        #         blosc_block_size=3*(1<<20),
        #         cname='zstd',
        #         clevel=1,
        #         shuffle='bitshuffle',
        #         typesize=4,
        #     ),
        #     truncbits=truncvel,
        # ),
        # ParticleIDs=dict(
        #     asdf=dict(
        #         compression='blsc',
        #         compression_block_size=12*(1<<20),
        #         blosc_block_size=3*(1<<20),
        #         cname='zstd',
        #         clevel=1,
        #         shuffle='shuffle',
        #         typesize=4,
        #     ),
        #     truncbits=0,
        # ),
    )

    for fn in src:
        fn = Path(fn)
        out = dst / (fn.stem + '_compressed.asdf')
        with h5py.File(fn, 'r') as h5:
            h5size = 0
            tree = dict(PartType1=dict())
            af = asdf.AsdfFile(tree)
            for name in compression_opts:
                p = h5[f'/PartType1/{name}'][:]

                tbits = compression_opts[name]['truncbits']
                mask = ~np.uint32((1 << tbits) - 1)
                p = (p.view(dtype=np.uint32) & mask).view(dtype=p.dtype)

                # p[1:] -= p[:-1]

                af['PartType1'][name] = p
                af.set_array_compression(
                    p, **compression_opts[name]['asdf'],
                )
                h5size += p.nbytes
            af.write_to(out)
        
        #h5size = fn.stat().st_size
        afsize = out.stat().st_size
        print(f'Input size:  {h5size/1e6:.4g} MB')
        print(f'Output size: {afsize/1e6:.4g} MB')
        print(f'Compression factor: {h5size/afsize:.3g}x')
        print(f'Options: {compression_opts}')


if __name__ == '__main__':
    compress()
