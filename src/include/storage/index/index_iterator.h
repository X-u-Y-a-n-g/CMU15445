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
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index = 0);
  
  IndexIterator();
  ~IndexIterator();  // NOLINT
  
  static IndexIterator End(){
    return IndexIterator();
  }

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { 
    //UNIMPLEMENTED("TODO(P2): Add implementation."); 
    return page_id_ == itr.page_id_ && index_ == itr.index_ ;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { 
    //UNIMPLEMENTED("TODO(P2): Add implementation."); 
    return  !(*this == itr);
  }

 private:
  // add your own private member variables here

  BufferPoolManager *bpm_;
  
  // 当前叶子节点的页面ID
  page_id_t page_id_{INVALID_PAGE_ID};
   
  // 当前叶子节点中的索引位置
  int index_{0};
   
  // 当前叶子页面的保护对象
  ReadPageGuard leaf_guard_;
   
  // 叶子节点指针，指向当前页面
  const LeafPage *leaf_page_;
};

}  // namespace bustub