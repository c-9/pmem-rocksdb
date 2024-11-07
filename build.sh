#!/usr/bin/env bash
export PMDK_INCLUDE_PATH=/usr/local/pmdk-2.1.0/include
export PMDK_LIBRARY_PATH=/usr/local/pmdk-2.1.0/lib
sudo INSTALL_PATH=/usr/local/pmem-rocksdb-6.11.4 make USE_RTTI=1 DISABLE_WARNING_AS_ERROR=1 EXTRA_CFLAGS="-fPIC" EXTRA_CXXFLAGS="-fPIC" install-static -j 48
