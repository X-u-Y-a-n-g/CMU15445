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
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index)
  : bpm_ (bpm) ,
  page_id_(page_id),
  index_(index){
  leaf_guard_ = bpm_->ReadPage(page_id_);
  leaf_page_ = leaf_guard_.As<LeafPage>();
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  return page_id_ == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  if(leaf_page_ == nullptr || index_ >= leaf_page_->GetSize()  ){
    throw Exception(" * wrong !");
  }
  return {leaf_page_->KeyAt(index_),leaf_page_->ValueAt(index_)};
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  //将i_ndex位置进行移动
  index_++;
  if(index_ >= leaf_page_->GetSize()){
    page_id_t next_page_id = leaf_page_->GetNextPageId();
    // 释放当前页面
    leaf_guard_ = ReadPageGuard{};
    // 如果有下一个页面
    if (next_page_id != INVALID_PAGE_ID) {
      // 读取下一个页面
      leaf_guard_ = bpm_->ReadPage(next_page_id);
      leaf_page_ = leaf_guard_.As<LeafPage>();
      page_id_ = next_page_id;
      index_ = 0;
    } else {
      
      // 没有下一个页面，设置为无效状态
      page_id_ = INVALID_PAGE_ID;
      leaf_page_ = nullptr;
      index_ = 0;
    }  
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub