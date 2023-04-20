ml modules/2.0
ml python/3.10
ml hdf5
ml disBatch
ml gcc

if [[ ! -f "venv/bin/activate" ]]; then
    python -m venv venv --system-site-packages
fi

. venv/bin/activate
