//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  // 检查页面ID是否为INVALID_PAGE_ID(表示到达末尾)
  return page_id_ == INVALID_PAGE_ID;
  }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  // 检查是否到达末尾
  assert(!IsEnd());
  
  // 从缓冲池获取当前叶子页面
  auto page_guard = buffer_pool_manager_->ReadPage(page_id_);
  auto *leaf_page = page_guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  
  // 检查索引的有效性
  assert(index_ >= 0 && index_ < leaf_page->GetSize());
  
  // 返回当前位置的键值对
  return {leaf_page->KeyAt(index_), leaf_page->ValueAt(index_)};

}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  // 检查是否已经到达末尾
  if (IsEnd()) {
    return *this;
  }

  // 从缓冲池获取当前叶子页面
  auto page_guard = buffer_pool_manager_->ReadPage(page_id_);
  auto *leaf_page = page_guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

  // 移动到下一个位置
  index_++;

  // 如果到达当前页面末尾，需要移动到下一个页面
  if (index_ >= leaf_page->GetSize()) {
    // 获取下一个页面的ID
    page_id_t next_page_id = leaf_page->GetNextPageId();
    
    // 如果没有下一个页面，设置为End迭代器
    if (next_page_id == INVALID_PAGE_ID) {
      page_id_ = INVALID_PAGE_ID;
      index_ = 0;
      return *this;
    }
    
    // 移动到下一个页面的起始位置
    page_id_ = next_page_id;
    index_ = 0;
  }

  return *this;

}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
