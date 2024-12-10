#!/usr/bin/env bash
export PMDK_INCLUDE_PATH=/usr/local/pmdk-2.1.0/include
export PMDK_LIBRARY_PATH=/usr/local/pmdk-2.1.0/lib
export BENCHMARKS="db_bench table_reader_bench cache_bench memtablerep_bench filter_bench persistent_cache_bench range_del_aggregator_bench"
#sudo INSTALL_PATH=/usr/local/pmem-rocksdb-6.11.4 make DEBUG_LEVEL=2 USE_RTTI=1 DISABLE_WARNING_AS_ERROR=1 EXTRA_CFLAGS="-fPIC" EXTRA_CXXFLAGS="-fPIC" $BENCHMARKS install-shared -j 48
sudo INSTALL_PATH=/usr/local/pmem-rocksdb-6.11.4 make DEBUG_LEVEL=0 USE_RTTI=1 DISABLE_WARNING_AS_ERROR=1 EXTRA_CFLAGS="-fPIC" EXTRA_CXXFLAGS="-fPIC" $BENCHMARKS install-shared -j 48
