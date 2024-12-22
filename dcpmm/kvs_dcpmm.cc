//  Copyright (c) 2019, Intel Corporation. All rights reserved.
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef ON_DCPMM

#include "rocksdb/kvs_dcpmm.h"

#include <libpmem.h>
#include <libpmemobj.h>
#include <snappy.h>

#include <atomic>
#include <iostream>
#include <string>
#include <vector>
#include <thread>

#include "util/compression.h"

namespace rocksdb {

struct Pool {
  PMEMobjpool* pool;
  uint64_t uuid_lo;
  size_t base_addr;

  Pool() : pool(nullptr) {
  }

  ~Pool() {
    if (pool) {
      pmemobj_close(pool);
    }
  }
};

struct PobjAction : pobj_action {
  size_t pool_index;
};

static struct Pool* pools_ = nullptr;
static size_t pool_count_;
static std::atomic<size_t> next_pool_index_(0);

static size_t kvs_value_thres_ = 0;
static bool compress_value_ = false;
static size_t dcpmm_avail_size_min_ = 0;

static std::atomic<size_t> dcpmm_avail_size_(0);
static std::atomic<bool> dcpmm_is_avail_(true);

int KVSOpen(const char* path, size_t size, size_t pool_count) {
  assert(!pools_);
  pools_ = new struct Pool[pool_count];
  pool_count_ = pool_count;

  size_t pool_size = size / pool_count;
  for (size_t i = 0; i < pool_count; i++) {
    std::string pool_path(path);
    pool_path.append(".").append(std::to_string(i));

    struct KVSRoot {
      size_t size;
    };
    PMEMoid root;
    KVSRoot *rootp;
    auto* pool = pmemobj_create(pool_path.data(), "store_rocksdb_value",
                               pool_size, 0666);
    if (pool) {
#ifdef POOL_POPULATE
      if (!PopulatePool(pool, pool_size)) {
        fprintf(stderr, "Failed to populate pool %zu\n", i);
        pmemobj_close(pool);
        delete[] pools_;
        pools_ = nullptr;
        return -ENOMEM;
      }
#endif
      root = pmemobj_root(pool, sizeof(struct KVSRoot));
      rootp = (struct KVSRoot*)pmemobj_direct(root);
      rootp->size = pool_size;
      pmemobj_persist(pool, &(rootp->size), sizeof(rootp->size));
    }
    else {
      pool = pmemobj_open(pool_path.data(), "store_rocksdb_value");
      if (pool == nullptr) {
        delete[] pools_;
        pools_ = nullptr;
        return -EIO;
      }
      root = pmemobj_root(pool, sizeof(struct KVSRoot));
      rootp = (struct KVSRoot*)pmemobj_direct(root);
    }

    pools_[i].pool = pool;
    pools_[i].uuid_lo = root.pool_uuid_lo;
    pools_[i].base_addr = (size_t)pool;
  }
  // hard code it as 1/10 of total dcpmm size.
  dcpmm_avail_size_min_ = size / 10;
  return 0;
}

bool PopulatePool(PMEMobjpool* pool, size_t pool_size) {
  const size_t page_size = sysconf(_SC_PAGESIZE);
  char* base_addr = (char*)pmemobj_direct(pmemobj_root(pool, 1));
  
  // Multi-threaded population
  const int num_threads = std::thread::hardware_concurrency();
  std::vector<std::thread> threads;
  
  size_t chunk_size = pool_size / num_threads;
  chunk_size = (chunk_size + page_size - 1) & ~(page_size - 1); // Align to page size

  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back([=]() {
      char* start = base_addr + (i * chunk_size);
      size_t len = (i == num_threads - 1) ? 
                   pool_size - (i * chunk_size) : 
                   chunk_size;

      for (size_t offset = 0; offset < len; offset += page_size) {
        // Use non-temporal stores for better performance
        _mm512_stream_si512(
            reinterpret_cast<__m512i*>(start + offset),
            _mm512_set1_epi64(0ULL));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
  
  _mm_mfence();

  // Verify the population
  if (!VerifyPoolPopulation(base_addr, pool_size)) {
    fprintf(stderr, "Pool population verification failed\n");
    return false;
  }
  return true;
}

bool VerifyPoolPopulation(char* base_addr, size_t pool_size) {
  const size_t page_size = sysconf(_SC_PAGESIZE);
  size_t num_pages = (pool_size + page_size - 1) / page_size;
  std::vector<unsigned char> vec(num_pages);

  if (mincore(base_addr, pool_size, vec.data()) != 0) {
    fprintf(stderr, "mincore failed: %s\n", strerror(errno));
    return false;
  }

  size_t resident_pages = 0;
  std::vector<size_t> missing_pages;
  
  for (size_t i = 0; i < num_pages; i++) {
    if (vec[i] & 1) {
      resident_pages++;
    } else {
      // Keep track of first few missing pages for debugging
      if (missing_pages.size() < 10) {
        missing_pages.push_back(i);
      }
    }
  }

  if (resident_pages != num_pages) {
    fprintf(stderr, "Pool population incomplete: %zu/%zu pages resident\n",
            resident_pages, num_pages);
    if (!missing_pages.empty()) {
      fprintf(stderr, "First few missing pages: ");
      for (size_t page : missing_pages) {
        fprintf(stderr, "%zu ", page);
      }
      fprintf(stderr, "\n");
    }
    return false;
  }

  return true;
}

void KVSClose() {
  delete[] pools_;
  pools_ = nullptr;
}

enum ValueEncoding KVSGetEncoding(const void *ptr) {
  // whether it is raw or pointed, the first byte is encoding
  auto* hdr = (KVSHdr*)ptr;
  return (enum ValueEncoding)(hdr->encoding);
}

bool KVSEnabled() {
  return pools_ != nullptr;
}

static bool ReservePmem(size_t size, size_t* p_pool_index, PMEMoid* p_oid,
                        struct PobjAction* pact) {
  size_t pool_index = (next_pool_index_++) % pool_count_;
  size_t retry_loop = pool_count_;

  PMEMoid oid;
  for (size_t i = 0; i < retry_loop; i++) {
    auto* pool = pools_[pool_index].pool;
    oid = pmemobj_reserve(pool, pact, size, 0);
    if (!OID_IS_NULL(oid)) {
      *p_pool_index = pool_index;
      *p_oid = oid;
      pact->pool_index = pool_index;
      return true;
    }
    pool_index++;
    if (pool_index >= pool_count_) {
      pool_index = 0;
    }
  }

  dcpmm_is_avail_ = false;
  return false;
}

bool KVSEncodeValue(const Slice& value, bool compress,
                    struct KVSRef* ref) {
  assert(pools_);

  // If dcpmm has not enough space, the caller need to fallback to non-kvs.
  if (!dcpmm_is_avail_) {
    return false;
  }

  PobjAction pact;

  if (!compress) {
    PMEMoid oid;
    if (!ReservePmem(sizeof(struct KVSHdr) + value.size(), &(ref->pool_index),
                      &oid, &pact)) {
      return false;
    }
    void *buf = pmemobj_direct(oid);
    ref->hdr.encoding = kEncodingPtrUncompressed;
    ref->size = value.size();
    assert((size_t)buf >= pools_[ref->pool_index].base_addr);
    ref->off_in_pool = (size_t)buf - pools_[ref->pool_index].base_addr;

    // Prefix the encoding type of the value content.
    memcpy(buf, &(ref->hdr), sizeof(ref->hdr));
    memcpy((char*)buf + sizeof(ref->hdr), value.data(), value.size());
    pmemobj_persist(pools_[ref->pool_index].pool, buf, value.size() + sizeof(ref->hdr));
    pmemobj_publish(pools_[ref->pool_index].pool, (pobj_action*)&pact, 1);
// #ifndef NDEBUG
//     printf("[PMEM] uncompressed value size %lu\n", value.size());
// #endif
  } else {
    // So far, just support to use snappy for value compression.
#ifdef SNAPPY
    char *compressed = new char[snappy::MaxCompressedLength(value.size())];
    size_t outsize;
    snappy::RawCompress(value.data(), value.size(), compressed, &outsize);
#else
    fprintf(stderr, "Doesn't support snappy.\n");
    assert(0);
#endif

    PMEMoid oid;
    if (!ReservePmem(sizeof(struct KVSHdr) + outsize, &(ref->pool_index),
                      &oid, &pact)) {
      delete[] compressed;
      return false;
    }
    void *buf = pmemobj_direct(oid);
    // Fill a header structure, and the caller will insert it instead of the
    // original value.
    ref->hdr.encoding = kEncodingPtrCompressed;
    ref->size = outsize;
    assert((size_t)buf >= pools_[ref->pool_index].base_addr);
    ref->off_in_pool = (size_t)buf - pools_[ref->pool_index].base_addr;

    // Prefix the encoding type of value content.
    memcpy(buf, &(ref->hdr), sizeof(ref->hdr));
    memcpy((char*)buf + sizeof(ref->hdr), compressed, outsize);
    pmemobj_persist(pools_[ref->pool_index].pool, buf, outsize + sizeof(ref->hdr));
    pmemobj_publish(pools_[ref->pool_index].pool, (pobj_action*)&pact, 1);
    delete[] compressed;
// #ifndef NDEBUG
//     printf("[PMEM] compressed value size %lu\n", outsize);
// #endif
  }

  return true;
}

static void FreePmem(struct KVSRef* ref) {
  PMEMoid oid;
  oid.pool_uuid_lo = pools_[ref->pool_index].uuid_lo;
  oid.off = ref->off_in_pool;
  pmemobj_free(&oid);
  if (!dcpmm_is_avail_) {
    if ((dcpmm_avail_size_ += ref->size) > dcpmm_avail_size_min_) {
      dcpmm_avail_size_ = 0;
      dcpmm_is_avail_ = true;
    }
  }
}
Slice KVSDumpFromValueRef(const Slice& value,
                           std::function<void(const Slice& value)> add) {
  assert(pools_);
  const char* input = value.data();
  auto* ref = (struct KVSRef*)input;
  if (ref->hdr.encoding == kEncodingPtrCompressed ||
      ref->hdr.encoding == kEncodingPtrUncompressed) {
    auto* hdr = (struct KVSHdr*)(pools_[ref->pool_index].base_addr
                                      + ref->off_in_pool);
    if (ref->hdr.encoding == kEncodingPtrCompressed) {
      hdr->encoding = kEncodingRawCompressed;
    }
    else {
      hdr->encoding = kEncodingRawUncompressed;
    }

    // Prefix encoding type of the value content.
    Slice v((char*)hdr, ref->size + sizeof(struct KVSHdr));
//    std::cerr<<"dump v size "<<v.size();
    add(v);

    FreePmem(ref);
    return v;
  }
  return value;
}

void KVSDecodeValueRef(const char* input, size_t size, std::string* dst) {
  assert(input);
  auto encoding = KVSGetEncoding(input);
  const char* src_data;
  size_t src_len;

  // if it is indirectly pointed, input is a KVSRef
  if (encoding == kEncodingPtrUncompressed ||
        encoding == kEncodingPtrCompressed) {
    assert(size == sizeof(struct KVSRef));
    auto* ref = (struct KVSRef*)input;
    // the data on DCPMM
    src_data = (char*)pools_[ref->pool_index].base_addr +
                ref->off_in_pool + sizeof(struct KVSHdr);
    src_len = ref->size;
  }
  // else it is directly referred, input is a KVSHdr with raw data following
  else {
    assert(encoding == kEncodingRawUncompressed ||
            encoding == kEncodingRawCompressed);
    assert(size >= sizeof(struct KVSHdr));
    src_data = input + sizeof(struct KVSHdr);
    src_len = size - sizeof(struct KVSHdr);
  }

  // if not compressed
  if (encoding == kEncodingRawUncompressed ||
      encoding == kEncodingPtrUncompressed) {
    dst->assign(src_data, src_len);
  }
  // else need to decompress
  else
  {
    assert(encoding == kEncodingRawCompressed ||
      encoding == kEncodingPtrCompressed);
    size_t dst_len;
    if (Snappy_GetUncompressedLength(src_data, src_len, &dst_len)) {
      char* tmp_buf = new char[dst_len];
      Snappy_Uncompress(src_data, src_len, tmp_buf);
      dst->assign(tmp_buf, dst_len);
      delete[] tmp_buf;
    } else {
      abort();
    }
  }
}

size_t KVSGetExtraValueSize(const Slice& value) {
  auto* ref = (struct KVSRef*)value.data();
  if (ref->hdr.encoding == kEncodingRawCompressed ||
      ref->hdr.encoding == kEncodingRawUncompressed) {
    return 0;
  }
  else {
    return (size_t)ref->size;
  }
}

void KVSFreeValue(const Slice& value) {
  auto* ref = (struct KVSRef*)value.data();
  if (ref && ref->hdr.encoding != kEncodingRawCompressed &&
      ref->hdr.encoding != kEncodingRawUncompressed) {
    FreePmem(ref);
  }
}

void KVSSetKVSValueThres(size_t thres) {
  kvs_value_thres_ = thres;
}

size_t KVSGetKVSValueThres() {
  return kvs_value_thres_;
}

void KVSSetCompressKnob(bool compress) {
  compress_value_ = compress;
}

bool KVSGetCompressKnob() {
  return compress_value_;
}

}  // namespace rocksdb
#endif
