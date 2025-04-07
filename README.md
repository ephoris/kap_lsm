# Kapacity LSM

This repository holds a RocksDB interface for a custom compaction policy following the
KapacityLSM design in [Towards Flexibility and Robustness of LSM Trees](https://doi.org/10.1007/s00778-023-00826-9).


## Compiling Building

Make sure to initialize the submodule that points to the RocksDB fork from our lab.

```
git submodule update --init --recursive
```

Afterwards we can simply build with cmake

```
cmake -S . -B build
cmake --build build
```

## Executables

We provide three executables for testing workloads on RocksDB, `gen_keys`, `build_db`,
and `run_db`. We assume the following workflow

1. Generate keys using `gen_keys`, note please generate a decent number of `extra_keys`
   in order to ensure non-empty reads are within the key domain.

2. Build your database using `build_db`

3. Run a workload using `run_db`

To see all of the arguments run `--help` on any executab

