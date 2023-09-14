#!/usr/bin/env python3

import json
from pathlib import Path
from timeit import default_timer

import click
import h5py
import hdf5plugin
import numpy as np


TRUNC_LEVELS = {
    # (box, n1d): (truncpos, truncvel)
    (1e6,1024): (6,11),
    (1e6,512):  (7,11),
    (1e6,256):  (8,11),
    (25e3,256): (8,11),
    }


@click.command()
@click.argument('src', nargs=-1)
@click.argument('dst')
@click.option('--truncpos', '-p', default='auto',
    help='Number of low bits to null out in the position data',
)
@click.option('--truncvel', '-v', default='auto',
    help='Number of low bits to null out in the velocity data',
)
@click.option('--verbose', '-V', is_flag=True, default=False)
def compress(src, dst, truncpos='auto', truncvel='auto', verbose=False):
    dst = Path(dst)
    src = [Path(fn) for fn in src]
    validate_paths(src, dst)
    dst.mkdir(parents=True, exist_ok=True)

    for fn in src:
        t = -default_timer()
        out = (dst / fn.name).with_suffix('.inprogress')

        # fail if 'inprogress' exists
        with h5py.File(fn, 'r') as h5in, h5py.File(out, 'w-') as h5out:

            validate_input(h5in)
            compression_opts = get_compression_opts(h5in['/Header'].attrs,
                truncpos, truncvel,
                )

            h5size = 0
            
            h5in.copy(h5in['/Header'], h5out['/'], 'Header')
            h5out.create_group('/CompressionInfo')
            h5out['/CompressionInfo'].attrs['json'] = json.dumps(compression_opts)

            for i in range(6):
                if f'/PartType{i}' not in h5in:
                    continue
                # iord = np.argsort(h5in[f'/PartType{i}/ParticleIDs'][:])
                for name in h5in[f'/PartType{i}'].keys():
                    p = h5in[f'/PartType{i}/{name}'][:]

                    if not np.issubdtype(p.dtype, np.integer):
                        tbits = 0  # compression_opts[name]['truncbits']
                        mask = ~np.uint32((1 << tbits) - 1)
                        p = (p.view(dtype=np.uint32) & mask).view(dtype=p.dtype)
                        copt = hdf5plugin.SZ3(relative=2**-13)
                    else:
                        copt = hdf5plugin.SZ3(absolute=0)

                    h5out.create_dataset(f'/PartType{i}/{name}', data=p,
                        # **compression_opts[name]['hdf5'],
                        # chunks = (1<<16,3) if p.ndim > 1 else (1<<16,),
                        # **hdf5plugin.Blosc(cname='zstd',
                        #                     clevel=5,
                        #                     shuffle=hdf5plugin.Blosc.SHUFFLE,
                        #                     ),
                        **copt
                        )
                    h5size += p.nbytes

        #insize = fn.stat().st_size
        outsize = out.stat().st_size
        t += default_timer()
        if verbose:
            print(f'Input size:  {h5size/1e6:.4g} MB')
            print(f'Output size: {outsize/1e6:.4g} MB')
            print(f'Compression factor: {h5size/outsize:.3g}x')
            print(f'Compression speed: {h5size/t/1e6:.3g} MB/s')
            print(f'Options: {compression_opts}')
        
        if outsize > h5size:
            raise RuntimeError(f'Compressed size {outsize} greater than uncompressed size {h5size}')

        out.chmod(0o444)
        out.rename(out.with_suffix('.hdf5'))


def nearest_boxsize(box):
    '''Boxsize to the nearest factor of two relative to 1e6
    '''
    if box == 25000.:  # CAMELS boxes
        return box
    return 2**np.round(np.log2(box/1e6))*1e6


def get_compression_opts(attrs, truncpos, truncvel, clevel=5):

    box = attrs['BoxSize']
    rounded_box = nearest_boxsize(box)
    n1d = int(round(attrs['NumPart_Total'][1]**(1/3)))

    if truncpos == 'auto':
        truncpos = TRUNC_LEVELS[(rounded_box,n1d)][0]
    if truncvel == 'auto':
        truncvel = TRUNC_LEVELS[(rounded_box,n1d)][1]

    compression_opts = dict(
        Coordinates=dict(
            hdf5=dict(
                chunks = (1<<16,3),
                **hdf5plugin.Blosc(cname='zstd',
                                   clevel=str(clevel),
                                   shuffle=hdf5plugin.Blosc.BITSHUFFLE,
                                   ),
            ),
            truncbits=truncpos,
        ),
        Velocities=dict(
            hdf5=dict(
                chunks = (1<<16,3),
                **hdf5plugin.Blosc(cname='zstd',
                                   clevel=str(clevel),
                                   shuffle=hdf5plugin.Blosc.BITSHUFFLE,
                                   ),
            ),
            truncbits=truncvel,
        ),
        ParticleIDs=dict(
            hdf5=dict(
                chunks = (1<<16,),
                **hdf5plugin.Blosc(cname='zstd',
                                   clevel=str(clevel),
                                   shuffle=hdf5plugin.Blosc.SHUFFLE,
                                   ),
            ),
            truncbits=0,
        ),
    )

    return compression_opts


def validate_paths(sources: list[Path], dst):
    for fn in sources:
        if not fn.is_file():
            raise FileNotFoundError(fn)
        try:
            same = fn.parent.samefile(dst)
        except FileNotFoundError:
            same = False
        if same:
            raise ValueError('Source and dest are the same!')


def validate_input(h, camels=True):
    # do some schema validation
    KNOWN_HDF5_GROUPS = ['Header',
                         'PartType1',
                         'PartType1/Coordinates',
                         'PartType1/Velocities',
                         'PartType1/ParticleIDs',
                         'PartType2',
                         'PartType2/Coordinates',
                         'PartType2/Velocities',
                         'PartType2/ParticleIDs',
                         ]
    GROUPS_WITH_ATTRS = ['Header']
    def _check(g):
        if g not in KNOWN_HDF5_GROUPS:
            raise ValueError(g)
        if (g in GROUPS_WITH_ATTRS) != bool(h[g].attrs):
            raise ValueError(g)
        
    if not camels:
        h.visit(_check)
                        

if __name__ == '__main__':
    compress()
