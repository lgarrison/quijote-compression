#!/usr/bin/env python3

import json
from pathlib import Path

import click
import h5py
import hdf5plugin
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
            hdf5=dict(
                chunks = (1<<20,3),
                # TODO: test performance without threads, blocksize, typesize access
                **hdf5plugin.Blosc(cname='zstd',
                                   clevel='1',
                                   shuffle=hdf5plugin.Blosc.BITSHUFFLE,
                                   ),
            ),
            truncbits=truncpos,
        ),
        Velocities=dict(
            hdf5=dict(
                chunks = (1<<20,3),
                **hdf5plugin.Blosc(cname='zstd',
                                   clevel='1',
                                   shuffle=hdf5plugin.Blosc.BITSHUFFLE,
                                   ),
            ),
            truncbits=truncvel,
        ),
        ParticleIDs=dict(
            hdf5=dict(
                chunks = (1<<20,),
                **hdf5plugin.Blosc(cname='zstd',
                                   clevel='1',
                                   shuffle=hdf5plugin.Blosc.SHUFFLE,
                                   ),
            ),
            truncbits=0,
        ),
    )

    for fn in src:
        fn = Path(fn)
        out = dst / (fn.stem + '_compressed.hdf5')
        with h5py.File(fn, 'r') as h5in, h5py.File(out, 'w') as h5out:
            h5size = 0
            
            h5in.copy(h5in['/Header'], h5out['/'], 'Header')
            h5out.create_group('/CompressionInfo')
            h5out['/CompressionInfo'].attrs['json'] = json.dumps(compression_opts)
            for name in compression_opts:
                p = h5in[f'/PartType1/{name}'][:]

                tbits = compression_opts[name]['truncbits']
                mask = ~np.uint32((1 << tbits) - 1)
                p = (p.view(dtype=np.uint32) & mask).view(dtype=p.dtype)
                h5out.create_dataset(f'/PartType1/{name}', data=p,
                    **compression_opts[name]['hdf5'],
                    )
                h5size += p.nbytes
        
        #insize = fn.stat().st_size
        outsize = out.stat().st_size
        print(f'Input size:  {h5size/1e6:.4g} MB')
        print(f'Output size: {outsize/1e6:.4g} MB')
        print(f'Compression factor: {h5size/outsize:.3g}x')
        print(f'Options: {compression_opts}')


if __name__ == '__main__':
    compress()
