# quijote-compression
Compression applied to Quijote N-body data to save disk space.

## Usage
### Overview
The workflow is the following:
1. Run `prepare_job.py` to create a list of disBatch compression tasks
2. Run disBatch on this list
3. Run `prepare_merge_trees.py` to create a list disBatch copy tasks, which handles any leftovers, like plain text files
4. Run disBatch on this list
5. Verify that the results look sane, then move or delete the original files by hand.

### Configuration
If you are compressing a simulation with a new combination of box size and number of particles, you may need to add an entry to the `TRUNC_LEVELS` dict indicating the number of bits of truncation to perform.  Every factor-of-two increase in N1D calls for 1 fewer bit of position truncation (assuming the softening as a fraction of the particle spacing is fixed). Velocity truncation can probably stay fixed.

Also, if you are compressing IC files with a number of files per sim that hasn't been seen before, you may need to add an entry to `NOUT_IC` in `prepare_job.py`.

But in most cases, there's no configuration required.

### Example
```bash
# Set up the environment
$ git clone https://github.com/lgarrison/quijote-compression
$ cd quijote-compression
$ . env.sh
$ pip install -r requirements.txt

# Prepare a compression job
$ mkdir job01
$ ./prepare_job.py ~/ceph/Quijote/SnapshotsUncompressed/ ~/ceph/Quijote/SnapshotsCompressed/ > job01/tasks

# Launch the compression job
$ sbatch -D job01 -p cmbas -t 1-0 -N 16 --ntasks-per-node=10 disBatch -e tasks

# After the job finishes, check Slurm to make sure it didn't fail. Then prepare the merge job:
$ ./prepare_merge_trees.py ~/ceph/Quijote/SnapshotsUncompressed/ ~/ceph/Quijote/SnapshotsCompressed/ > job01/merge_tasks

# Launch the merge job
$ sbatch -D job01 -p cmbas -t 1-0 -N 4 --ntasks-per-node=10 disBatch -e merge_tasks

# Once that's done, inspect the relative sizes of the compressed and uncompressed directories as a sanity check
$ ls -ldh ~/ceph/Quijote/SnapshotsUncompressed/ ~/ceph/Quijote/SnapshotsCompressed/

# Send the uncompressed copy to tape
$ mv ~/ceph/Quijote/SnapshotsUncompressed/* /mnt/ceph/tape/fvillaescusa/Quijote/Snapshots/
```

## Repo organization
- Compression scripts
    - `compress_hdf5.py`: the main script used to compress HDF5 files
    - `compress_gadget.py`: used to compress Gadget files while simultaneously converting them to HDF5
- disBatch scripts
    - `prepare_job.py`: prepare a list of disBatch tasks for compression jobs
    - `prepare_merge_trees.py`: prepare a list of disBatch tasks to copy any leftover files, like plain text files we did not compress
