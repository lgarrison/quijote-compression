#!/usr/bin/env python3

from pathlib import Path

import blosc2
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
            blosc2=dict(
                codec=blosc2.Codec.ZSTD,
                clevel=1,
                typesize=4,
                nthreads=1,
                # blocksize=3*(1<<20),
                filters=[blosc2.Filter.DELTA,],
                filters_meta=[0,],
            ),
            truncbits=0,
        ),
        # Velocities=dict(
        #     hdf5=dict(
        #         chunks = (1<<20,3),
        #         **hdf5plugin.Blosc(cname='zstd',
        #                            clevel='1',
        #                            shuffle=hdf5plugin.Blosc.BITSHUFFLE,
        #                            )
        #     ),
        #     truncbits=truncvel,
        # ),
        # ParticleIDs=dict(
        #     hdf5=dict(
        #         chunks = (1<<20,),
        #         **hdf5plugin.Blosc(cname='zstd',
        #                            clevel='1',
        #                            shuffle=hdf5plugin.Blosc.SHUFFLE,
        #                            )
        #     ),
        #     truncbits=0,
        # ),
    )

    for fn in src:
        fn = Path(fn)
        #out = dst / (fn.stem + '_compressed.hdf5')
        out = dst / (fn.stem + '.hdf5')
        with h5py.File(fn, 'r') as h5in:
            h5size = 0
            outsize = 0
            for name in compression_opts:
                p = h5in[f'/PartType1/{name}'][:]

                tbits = compression_opts[name]['truncbits']
                mask = ~np.uint32((1 << tbits) - 1)
                p = (p.view(dtype=np.uint32) & mask).view(dtype=p.dtype)
                h5size += p.nbytes
                
                comp = blosc2.compress2(p, **compression_opts[name]['blosc2'])
                outsize += len(comp)
        
        #insize = fn.stat().st_size
        # outsize = out.stat().st_size
        print(f'Input size:  {h5size/1e6:.4g} MB')
        print(f'Output size: {outsize/1e6:.4g} MB')
        print(f'Compression factor: {h5size/outsize:.3g}x')
        print(f'Options: {compression_opts}')


if __name__ == '__main__':
    compress()
