//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { UNIMPLEMENTED("TODO(P2): Add implementation."); }

  auto operator!=(const IndexIterator &itr) const -> bool { UNIMPLEMENTED("TODO(P2): Add implementation."); }

 private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_;
  page_id_t page_id_;    // 当前页面ID
  int index_;            // 当前页面中的索引位置
  
  // 用于构造函数
  IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index) 
    : buffer_pool_manager_(bpm), page_id_(page_id), index_(index) {}


};

}  // namespace bustub
