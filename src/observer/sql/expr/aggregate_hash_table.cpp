/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/expr/aggregate_hash_table.h"

// ----------------------------------StandardAggregateHashTable------------------

RC StandardAggregateHashTable::add_chunk(Chunk &groups_chunk, Chunk &aggrs_chunk)
{
  
  for (int i = 0; i < groups_chunk.rows(); i++) {
    std::vector<Value> key(groups_chunk.column_num());
    for (int j = 0; j < groups_chunk.column_num(); j++) {
      key[j].set_value(groups_chunk.column(j).get_value(i)); 
    }

    if (aggr_values_.count(key)) {
      auto &entry = aggr_values_.at(key);
      
      for (size_t j = 0; j < aggr_types_.size(); j++) {
        if (aggr_types_[j] == AggregateExpr::Type::SUM) {
          if (aggrs_chunk.column(j).attr_type() == AttrType::INTS) {
            entry[j].set_int(entry[j].get_int() + aggrs_chunk.column(j).get_value(i).get_int());
          } else if (aggrs_chunk.column(j).attr_type() == AttrType::FLOATS) {
            entry[j].set_float(entry[j].get_float() + aggrs_chunk.column(j).get_value(i).get_float());
          } else {
            return RC::NOT_EXIST;
          }
        } else {
          return RC::NOT_EXIST;
        }
      }
    } else {
      vector<Value> values(aggr_types_.size());
      for (size_t j = 0; j < aggr_types_.size(); j++) {
        values[j].set_value(aggrs_chunk.column(j).get_value(i));
      }
      aggr_values_.emplace(key, values);
    }
  }

  return RC::SUCCESS;
}

void StandardAggregateHashTable::Scanner::open_scan()
{
  it_  = static_cast<StandardAggregateHashTable *>(hash_table_)->begin();
  end_ = static_cast<StandardAggregateHashTable *>(hash_table_)->end();
}

RC StandardAggregateHashTable::Scanner::next(Chunk &output_chunk)
{
  if (it_ == end_) {
    return RC::RECORD_EOF;
  }
  while (it_ != end_ && output_chunk.rows() <= output_chunk.capacity()) {
    auto &group_by_values = it_->first;
    auto &aggrs           = it_->second;
    for (int i = 0; i < output_chunk.column_num(); i++) {
      auto col_idx = output_chunk.column_ids(i);
      if (col_idx >= static_cast<int>(group_by_values.size())) {
        output_chunk.column(i).append_one((char *)aggrs[col_idx - group_by_values.size()].data());
      } else {
        output_chunk.column(i).append_one((char *)group_by_values[col_idx].data());
      }
    }
    it_++;
  }
  if (it_ == end_) {
    return RC::SUCCESS;
  }

  return RC::SUCCESS;
}

size_t StandardAggregateHashTable::VectorHash::operator()(const vector<Value> &vec) const
{
  size_t hash = 0;
  for (const auto &elem : vec) {
    hash ^= std::hash<string>()(elem.to_string());
  }
  return hash;
}

bool StandardAggregateHashTable::VectorEqual::operator()(const vector<Value> &lhs, const vector<Value> &rhs) const
{
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (rhs[i].compare(lhs[i]) != 0) {
      return false;
    }
  }
  return true;
}

// ----------------------------------LinearProbingAggregateHashTable------------------
#ifdef USE_SIMD
template <typename V>
RC LinearProbingAggregateHashTable<V>::add_chunk(Chunk &group_chunk, Chunk &aggr_chunk)
{
  if (group_chunk.column_num() != 1 || aggr_chunk.column_num() != 1) {
    LOG_WARN("group_chunk and aggr_chunk size must be 1.");
    return RC::INVALID_ARGUMENT;
  }
  if (group_chunk.rows() != aggr_chunk.rows()) {
    LOG_WARN("group_chunk and aggr _chunk rows must be equal.");
    return RC::INVALID_ARGUMENT;
  }
  add_batch((int *)group_chunk.column(0).data(), (V *)aggr_chunk.column(0).data(), group_chunk.rows());
  return RC::SUCCESS;
}

template <typename V>
void LinearProbingAggregateHashTable<V>::Scanner::open_scan()
{
  capacity_   = static_cast<LinearProbingAggregateHashTable *>(hash_table_)->capacity();
  size_       = static_cast<LinearProbingAggregateHashTable *>(hash_table_)->size();
  scan_pos_   = 0;
  scan_count_ = 0;
}

template <typename V>
RC LinearProbingAggregateHashTable<V>::Scanner::next(Chunk &output_chunk)
{
  if (scan_pos_ >= capacity_ || scan_count_ >= size_) {
    return RC::RECORD_EOF;
  }
  auto linear_probing_hash_table = static_cast<LinearProbingAggregateHashTable *>(hash_table_);
  while (scan_pos_ < capacity_ && scan_count_ < size_ && output_chunk.rows() <= output_chunk.capacity()) {
    int key;
    V   value;
    RC  rc = linear_probing_hash_table->iter_get(scan_pos_, key, value);
    if (rc == RC::SUCCESS) {
      output_chunk.column(0).append_one((char *)&key);
      output_chunk.column(1).append_one((char *)&value);
      scan_count_++;
    }
    scan_pos_++;
  }
  return RC::SUCCESS;
}

template <typename V>
void LinearProbingAggregateHashTable<V>::Scanner::close_scan()
{
  capacity_   = -1;
  size_       = -1;
  scan_pos_   = -1;
  scan_count_ = 0;
}

template <typename V>
RC LinearProbingAggregateHashTable<V>::get(int key, V &value)
{
  RC  rc          = RC::SUCCESS;
  int index       = (key % capacity_ + capacity_) % capacity_;
  int iterate_cnt = 0;
  while (true) {
    if (keys_[index] == EMPTY_KEY) {
      rc = RC::NOT_EXIST;
      break;
    } else if (keys_[index] == key) {
      value = values_[index];
      break;
    } else {
      index += 1;
      index %= capacity_;
      iterate_cnt++;
      if (iterate_cnt > capacity_) {
        rc = RC::NOT_EXIST;
        break;
      }
    }
  }
  return rc;
}

template <typename V>
RC LinearProbingAggregateHashTable<V>::iter_get(int pos, int &key, V &value)
{
  RC rc = RC::SUCCESS;
  if (keys_[pos] == LinearProbingAggregateHashTable<V>::EMPTY_KEY) {
    rc = RC::NOT_EXIST;
  } else {
    key   = keys_[pos];
    value = values_[pos];
  }
  return rc;
}

template <typename V>
void LinearProbingAggregateHashTable<V>::aggregate(V *value, V value_to_aggregate)
{
  if (aggregate_type_ == AggregateExpr::Type::SUM) {
    *value += value_to_aggregate;
  } else {
    ASSERT(false, "unsupported aggregate type");
  }
}

template <typename V>
void LinearProbingAggregateHashTable<V>::resize()
{
  capacity_ *= 2;
  std::vector<int> new_keys(capacity_);
  std::vector<V>   new_values(capacity_);

  for (size_t i = 0; i < keys_.size(); i++) {
    auto &key   = keys_[i];
    auto &value = values_[i];
    if (key != EMPTY_KEY) {
      int index = (key % capacity_ + capacity_) % capacity_;
      while (new_keys[index] != EMPTY_KEY) {
        index = (index + 1) % capacity_;
      }
      new_keys[index]   = key;
      new_values[index] = value;
    }
  }

  keys_   = std::move(new_keys);
  values_ = std::move(new_values);
}

template <typename V>
void LinearProbingAggregateHashTable<V>::resize_if_need()
{
  if (size_ >= capacity_ / 2) {
    resize();
  }
}

template <typename V>
void LinearProbingAggregateHashTable<V>::add_batch(int *input_keys, V *input_values, int len)
{
  for (int i = 0; i < len; i++) {
    int key = input_keys[i];
    V value = input_values[i];
    int index = (key % capacity_ + capacity_) % capacity_;
    while (keys_[index] != EMPTY_KEY && keys_[index] != key) {
      index = (index + 1) % capacity_;
    }
    if (keys_[index] == EMPTY_KEY) {
      keys_[index] = key;
      values_[index] = value;
    } else {
      aggregate(&values_[index], value);
    }
  }

  // resize_if_need();

  // __m256i inv = _mm256_set1_epi32(-1); // Initialize inv to all -1 (valid)
  // __m256i off = _mm256_setzero_si256(); // Initialize off to all 0
  // int i = 0;

  // for (; i + SIMD_WIDTH <= len;) {
  //   // 1. Selective load keys and values
  //   int keys[SIMD_WIDTH];
  //   V values[SIMD_WIDTH];
  //   selective_load<int>(input_keys, i, keys, inv);
  //   selective_load<V>(input_values, i, values, inv);

  //   i += _mm256_movemask_epi8(_mm256_cmpeq_epi32(inv, _mm256_set1_epi32(-1)));

  //   // 2. Compute hash values
  //   __m256i hash_vals = _mm256_setr_epi32(
  //     (keys[0] % capacity_ + capacity_) % capacity_,
  //     (keys[1] % capacity_ + capacity_) % capacity_,
  //     (keys[2] % capacity_ + capacity_) % capacity_,
  //     (keys[3] % capacity_ + capacity_) % capacity_,
  //     (keys[4] % capacity_ + capacity_) % capacity_,
  //     (keys[5] % capacity_ + capacity_) % capacity_,
  //     (keys[6] % capacity_ + capacity_) % capacity_,
  //     (keys[7] % capacity_ + capacity_) % capacity_
  //   );

  //   // 3. Add offset to hash values
  //   // hash_vals = _mm256_add_epi32(hash_vals, off);

  //   for (int j = 0; j < SIMD_WIDTH; ++j) {
  //     if (keys[j] != EMPTY_KEY) {
  //       int index = mm256_extract_epi32_var_indx(hash_vals, j);
  //       while (keys_[index] != EMPTY_KEY && keys_[index] != keys[j]) {
  //         index = (index + 1) % capacity_;
  //       }
  //       if (keys_[index] == EMPTY_KEY) {
  //         keys_[index] = keys[j];
  //         values_[index] = values[j];
  //       } else {
  //         aggregate(&values_[index], values[j]);
  //       }
  //     }
  //   }


  //    __m256i table_keys = _mm256_setzero_si256();
  //   for (int j = 0; j < SIMD_WIDTH; ++j) {
  //     if (keys[j] != EMPTY_KEY) {
  //       int index = mm256_extract_epi32_var_indx(hash_vals, j);
  //       table_keys = _mm256_insert_epi32(table_keys, keys_[index], j);
  //     }
  //   }

  //   for (int j = 0; j < SIMD_WIDTH; ++j) {
  //     if (keys[j] == table_keys[j]) {
  //       inv = _mm256_insert_epi32(inv, -1, j);
  //       off = _mm256_insert_epi32(off, 0, j);
  //     } else {
  //       inv = _mm256_insert_epi32(inv, 0, j);
  //       off = _mm256_insert_epi32(off, mm256_extract_epi32_var_indx(off, j) + 1, j);
  //     }
  //   }
  // }

  // for (; i < len; ++i) {
  //   int key = input_keys[i];
  //   V value = input_values[i];
  //   int index = (key % capacity_ + capacity_) % capacity_;
  //   while (keys_[index] != EMPTY_KEY && keys_[index] != key) {
  //     index = (index + 1) % capacity_;
  //   }
  //   if (keys_[index] == EMPTY_KEY) {
  //     keys_[index] = key;
  //     values_[index] = value;
  //   } else {
  //     aggregate(&values_[index], value);
  //   }
  // }

  // resize_if_need();
  
  // __m256i inv = _mm256_set1_epi32(-1);
  // __m256i off = _mm256_setzero_si256();

  // int i = 0;
  // for (; i + SIMD_WIDTH <= len; ) {
  //   int key[SIMD_WIDTH];
  //   V value[SIMD_WIDTH];

  //   selective_load<int>(input_keys, i, key, inv);
  //   selective_load<V>(input_values, i, value, inv);

  //   i += _mm256_movemask_epi8(_mm256_cmpeq_epi32(inv, _mm256_set1_epi32(-1)));

  //   __m256i hash_vals = _mm256_set_epi32(
  //     hash_func(key[7]), hash_func(key[6]), hash_func(key[5]), hash_func(key[4]),
  //     hash_func(key[3]), hash_func(key[2]), hash_func(key[1]), hash_func(key[0])
  //   );

  //   __m256i pos = _mm256_add_epi32(hash_vals, off);
  //   pos = _mm256_and_si256(pos, _mm256_set1_epi32(capacity_ - 1));
  //   __m256i cmp = _mm256_cmpeq_epi32(keys, _mm256_loadu_si256((__m256i*)key));

  //   inv = _mm256_blendv_epi8(_mm256_set1_epi32(0), inv, cmp); // Invalidate mismatched entries
  //   off = _mm256_add_epi32(off, _mm256_andnot_si256(cmp, _mm256_set1_epi32(1))); // Increment offsets for mismatches
     


  //   selective_load<V>(V *memory, int offset, V *vec, __m256i &inv);
  // }

  // resize_if_need();

  // inv (invalid) 表示是否有效，inv[i] = -1 表示有效，inv[i] = 0 表示无效。
  // key[SIMD_WIDTH],value[SIMD_WIDTH] 表示当前循环中处理的键值对。
  // off (offset) 表示线性探测冲突时的偏移量，key[i] 每次遇到冲突键，则off[i]++，如果key[i] 已经完成聚合，则off[i] = 0，
  // i = 0 表示selective load 的起始位置。
  // inv 全部初始化为 -1
  // off 全部初始化为 0

  // for (; i + SIMD_WIDTH <= len;) {
    // 1: 根据 `inv` 变量的值，从 `input_keys` 中 `selective load` `SIMD_WIDTH` 个不同的输入键值对。
    // 2. 计算 i += |inv|, `|inv|` 表示 `inv` 中有效的个数 
    // 3. 计算 hash 值，
    // 4. 根据聚合类型（目前只支持 sum），在哈希表中更新聚合结果。如果本次循环，没有找到key[i] 在哈希表中的位置，则不更新聚合结果。
    // 5. gather 操作，根据 hash 值将 keys_ 的 gather 结果写入 table_key 中。
    // 6. 更新 inv 和 off。如果本次循环key[i] 聚合完成，则inv[i]=-1，表示该位置在下次循环中读取新的键值对。
    // 如果本次循环 key[i] 未在哈希表中聚合完成（table_key[i] != key[i]），则inv[i] = 0，表示该位置在下次循环中不需要读取新的键值对。
    // 如果本次循环中，key[i]聚合完成，则off[i] 更新为 0，表示线性探测偏移量为 0，key[i] 未完成聚合，则off[i]++,表示线性探测偏移量加 1。
  // }
  //7. 通过标量线性探测，处理剩余键值对

  // resize_if_need();
}

template <typename V>
const int LinearProbingAggregateHashTable<V>::EMPTY_KEY = 0xffffffff;
template <typename V>
const int LinearProbingAggregateHashTable<V>::DEFAULT_CAPACITY = 16384;

template class LinearProbingAggregateHashTable<int>;
template class LinearProbingAggregateHashTable<float>;
#endif