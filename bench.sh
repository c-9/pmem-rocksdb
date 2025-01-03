#!/usr/bin/env bash


current_dir=$(dirname $0)

export PMDK_INCLUDE_PATH=/usr/local/pmdk-2.1.0/include
export PMDK_LIBRARY_PATH=/usr/local/pmdk-2.1.0/lib

$current_dir/db_bench -benchmarks="fillrandom" \
    -threads=1 \
    -num=100000 \
    -key_size=16 \
    -value_size=64 \
    -dcpmm_wal_enable=0 \
    -cache_index_and_filter_blocks_for_mmap_read=1 \
    -cache_data_blocks_for_mmap_read=0 \
    -wal_dir=/mnt/pmem2/zjq/rocksdb/wal \
    -db=/mnt/pmem2/zjq/rocksdb/db \
    -dcpmm_kvs_enable=0 \
    -dcpmm_kvs_mmapped_file_fullpath=/mnt/pmem2/zjq/rocksdb/kvs \
    -dcpmm_kvs_mmapped_file_size=4294967296

