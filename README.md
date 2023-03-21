# quijote-compression
Compression applied to Quijote N-body data to save disk space.

- Compression scripts
    - `compress_hdf5.py`: the main script used to compress HDF5 files
    - `compress_gadget.py`: used to compress Gadget files while simultaneously converting them to HDF5
- disBatch scripts
    - `prepare_job.py`: prepare a list of disBatch tasks for compression jobs
    - `prepare_merge_trees.py`: prepare a list of disBatch tasks to copy any leftover files, like plain text files we did not compress
