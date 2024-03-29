#!/usr/bin/env python3
'''
Compress Gadget files (IC or snap) to HDF5
'''

import json
from pathlib import Path
from timeit import default_timer

import click
import h5py
import hdf5plugin
import numpy as np
import readsnap

from compress_hdf5 import TRUNC_LEVELS


@click.command()
@click.argument('src', nargs=-1)
@click.argument('dst')
@click.option('truncpos', '-p', default='auto',
    help='Number of low bits to null out in the position data',
)
@click.option('truncvel', '-v', default='auto',
    help='Number of low bits to null out in the velocity data',
)
@click.option('verbose', '-V', is_flag=True, default=False)
@click.option('sort', '-s', is_flag=True, default=False)
def compress(src, dst, truncpos, truncvel, verbose=False, sort=False):
    t = -default_timer()
    dst = Path(dst)
    src = [Path(fn) for fn in src]
    validate_paths(src, dst)
    # dst.parents[1].chmod(0o755)
    dst.parent.mkdir(parents=True, exist_ok=True)

    all_headers = [vars(readsnap.snapshot_header(fn)) for fn in src]
    validate_headers(all_headers)
    header = to_hdf5_header(all_headers)

    compression_opts = get_compression_opts(header, truncpos, truncvel,
                        sort=sort,
                        )

    out = dst.with_suffix('.inprogress')
    insize = 0
    with h5py.File(out, 'w-') as h5out:
        h5out.create_group('/Header')
        for k in header:
            h5out['/Header'].attrs[k] = header[k]
        h5out.create_group('/CompressionInfo')
        h5out['/CompressionInfo'].attrs['json'] = json.dumps(compression_opts)

        for i in [1,2]:
            if (npart := header['NumPart_ThisFile'][i]) == 0:
                continue

            for name in ['ParticleIDs', 'Coordinates', 'Velocities']:
                opts = compression_opts[name]
                shape = (npart,3) if name in ('Coordinates','Velocities') else (npart,)
                tmp = np.empty(shape, dtype=opts['hdf5']['dtype'])
                
                blockname = opts['blockname']
                nwrite = 0
                for fn in src:
                    if 'ic' in fn.name:
                        assert opts['truncbits'] == 0
                    block = readsnap.read_block(fn, blockname, parttype=i,
                        physical_velocities=False,
                        )
                    insize += block.nbytes

                    tbits = opts['truncbits']
                    mask = ~np.uint32((1 << tbits) - 1)
                    block = (block.view(dtype=np.uint32) & mask).view(dtype=block.dtype)

                    tmp[nwrite : nwrite + len(block)] = block
                    nwrite += len(block)
                    del block
                assert nwrite == shape[0]

                if sort:
                    if name == 'ParticleIDs':
                        iord = np.argsort(tmp)
                    tmp = tmp[iord]
                h5out.create_dataset(f'/PartType{i}/{name}',
                    data=tmp,
                    **opts['hdf5'],
                    )
            if sort:
                del iord

    outsize = out.stat().st_size
    t += default_timer()
    if verbose:
        print(f'Input size:  {insize/1e6:.4g} MB')
        print(f'Output size: {outsize/1e6:.4g} MB')
        print(f'Compression factor: {insize/outsize:.3g}x')
        print(f'Compression speed: {insize/t/1e6:.3g} MB/s')
        print(f'Options: {compression_opts}')
    
    if outsize > insize:
        raise RuntimeError(f'Compressed size {outsize} greater than uncompressed size {insize}')

    out.chmod(0o444)
    out.rename(out.with_suffix('.hdf5'))


def get_compression_opts(header, truncpos, truncvel, clevel=5, sort=False):

    box = header['BoxSize']
    n1d = int(round(header['NumPart_Total'][1]**(1/3)))

    if truncpos == 'auto':
        truncpos = TRUNC_LEVELS[(box,n1d)][0]
    if truncvel == 'auto':
        truncvel = TRUNC_LEVELS[(box,n1d)][1]

    compression_opts = dict(
        Coordinates=dict(
            hdf5=dict(
                chunks = (1<<16,3),
                dtype = 'f4',
                **hdf5plugin.Blosc(cname='zstd',
                                   clevel=str(clevel),
                                   shuffle=hdf5plugin.Blosc.BITSHUFFLE,
                                   ),
            ),
            truncbits=int(truncpos),
            blockname="POS ",
        ),
        Velocities=dict(
            hdf5=dict(
                chunks = (1<<16,3),
                dtype = 'f4',
                **hdf5plugin.Blosc(cname='zstd',
                                   clevel=str(clevel),
                                   shuffle=hdf5plugin.Blosc.BITSHUFFLE,
                                   ),
            ),
            truncbits=int(truncvel),
            blockname="VEL ",
        ),
        ParticleIDs=dict(
            hdf5=dict(
                chunks = (1<<16,),
                dtype = 'u4',
                **hdf5plugin.Blosc(cname='zstd',
                                   clevel=str(clevel),
                                   shuffle=hdf5plugin.Blosc.SHUFFLE,
                                   ),
            ),
            truncbits=0,
            blockname="ID  ",
        ),
        sort=sort,
    )

    return compression_opts


def validate_paths(sources: list[Path], dst: Path):
    for fn in sources:
        if not fn.is_file():
            raise FileNotFoundError(fn)
        try:
            same = fn.parent.samefile(dst.parent)
        except FileNotFoundError:
            same = False
        if same:
            raise ValueError('Source and dest are the same!')



def validate_headers(headers):
    # ['filename', 'format', 'swap', 'npart', 'massarr', 'time', 'redshift',
    # 'sfr', 'feedback', 'nall', 'cooling', 'filenum', 'boxsize', 'omega_m',
    # 'omega_l', 'hubble']

    for header in headers:
        # assert re.match(r'ics.\d+', str(header['filename'].name))
        assert (header['npart'] <= header['nall']).all()
        assert np.all((header['massarr'] > 0) == (header['nall'] > 0))
        assert header['time'] > 0
        assert header['redshift'] > 0
        assert header['nall'].sum() in (256**3, 512**3, 1024**3, 512**3 * 2)
        assert header['filenum'] in (8,16,64,128,512)
        assert (header['filenum'] // len(headers)) * len(headers) == header['filenum']
        # assert header['boxsize'] == 1e6

        # Only Type 1, and maybe Type 2
        assert np.all(header['nall'][[0,3,4,5]] == 0)
        assert header['nall'][1] > 0


def to_hdf5_header(headers):
    # combine all npart
    hin = headers[0]
    nfiles = hin['filenum'] // len(headers)
    npart = np.array([h['npart'] for h in headers], dtype=np.int32).sum(axis=0, dtype=np.int32)
    
    hout = dict(
            BoxSize = np.float64(hin['boxsize']),
            Flag_Cooling = np.int32(hin['cooling']),
            Flag_DoublePrecision = np.int32(0),  # NB not in readsnap header
            Flag_Feedback = np.int32(hin['feedback']),
            # Flag_IC_Info = np.int32(3),  # NB not in readsnap header
            Flag_Metals = np.int32(0),  # NB not in readsnap header
            Flag_Sfr = np.int32(hin['sfr']),
            Flag_StellarAge = np.int32(0),  # NB not in readsnap header
            HubbleParam = np.float64(hin['hubble']),
            MassTable = np.array(hin['massarr'], dtype=np.float64),
            NumFilesPerSnapshot = np.int32(nfiles),
            NumPart_ThisFile = npart,
            NumPart_Total = np.array(hin['nall'], dtype=np.uint32),
            NumPart_Total_HighWord = np.array([0, 0, 0, 0, 0, 0], dtype=np.uint32),  # no quijote simulations use this
            Omega0 = np.float64(hin['omega_m']),
            OmegaLambda = np.float64(hin['omega_l']),
            Redshift = np.float64(hin['redshift']),
            Time = np.float64(hin['time']),
    )
    return hout

if __name__ == '__main__':
    compress()
