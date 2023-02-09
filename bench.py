#!/usr/bin/env python3

import timeit

import h5py
import hdf5plugin


def bench(fn):
    h = h5py.File(fn,
        # rdcc_nbytes=0,
    )
    elapsed = -timeit.default_timer()
    size = 0
    a = h['/PartType1/Coordinates'][:]
    size += a.nbytes
    # a = h['/PartType1/Velocities'][:]
    # size += a.nbytes
    # a = h['/PartType1/ParticleIDs'][:]
    # size += a.nbytes
    elapsed += timeit.default_timer()
    print(f'Time: {elapsed:.4g} sec')
    print(f'Rate: {size/elapsed/1e6:.4g} MB/s')

if __name__ == '__main__':
    bench('data/snapdir_000/snap_000.1.hdf5')
    bench('out/snap_000.1.hdf5')
    # bench('/dev/shm/snap.hdf5')
    # bench('/dev/shm/snap.comp.hdf5')
