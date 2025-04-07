//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  // 创建Context实例
  Context ctx;
  
  // 获取header page的读锁
  ctx.header_page_ = bpm_->ReadPage(header_page_id_);
  
  // 通过header page获取root page id
  auto *header_page = ctx.header_page_->As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  
  // root_page_id_为INVALID_PAGE_ID表示树为空
  return ctx.root_page_id_ == INVALID_PAGE_ID;
  
  // header_page_的guard会在函数返回时自动释放
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with input key, if the value exists
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;

  // 检查树是否为空
  if (IsEmpty()) {
    return false;
  }

  // 获取header page并读取root page id
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto *header_page = header_guard.As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  
  // 从root开始查找
  auto guard = bpm_->ReadPage(ctx.root_page_id_);
  auto *page = guard.As<BPlusTreePage>();
  ctx.read_set_.push_back(std::move(guard));

  // 遍历直到找到叶子节点
  while (!page->IsLeafPage()) {
    auto *internal = ctx.read_set_.back().As<InternalPage>();
    // 查找下一层的page id
    page_id_t next_page_id = internal->Lookup(key, comparator_);
    // 获取下一层节点并加入read set
    auto next_guard = bpm_->ReadPage(next_page_id);
    page = next_guard.As<BPlusTreePage>();
    ctx.read_set_.push_back(std::move(next_guard));
  }

  // 在叶子节点中查找key
  auto *leaf = ctx.read_set_.back().As<LeafPage>();
  ValueType value;
  if (leaf->Lookup(key, &value, comparator_)) {
    result->push_back(value);
    return true;
  }

  return false;
  // read_set_ 中的页面锁会在函数返回时自动释放
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 *
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 *
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;

  // 如果是空树,创建新的根节点
  if (IsEmpty()) {
    // 获取header page的写锁
    ctx.header_page_ = bpm_->WritePage(header_page_id_);
    auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
    
    // 创建新的叶子节点作为根节点
    auto guard = bpm_->NewPage();
    auto root = guard.AsMut<LeafPage>();
    root->Init(guard.PageId());
    
    // 在新的根节点中插入键值对
    if (!root->Insert(key, value, comparator_)) {
      return false;  // 插入失败(不应该发生)
    }
    
    // 更新header page中的root page id
    header_page->root_page_id_ = guard.PageId();
    ctx.root_page_id_ = guard.PageId();
    
    return true;
  }

  // 获取header page并读取root page id
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  
  // 从root开始查找插入位置
  auto guard = bpm_->WritePage(ctx.root_page_id_);
  auto *page = guard.AsMut<BPlusTreePage>();
  ctx.write_set_.push_back(std::move(guard));

  // 遍历到叶子节点
  while (!page->IsLeafPage()) {
    auto *internal = ctx.write_set_.back().AsMut<InternalPage>();
    page_id_t next_page_id = internal->Lookup(key, comparator_);
    auto child_guard = bpm_->WritePage(next_page_id);
    page = child_guard.AsMut<BPlusTreePage>();
    ctx.write_set_.push_back(std::move(child_guard));
  }

  // 获取叶子节点
  auto *leaf = ctx.write_set_.back().AsMut<LeafPage>();
  
  // 检查是否存在重复的key
  ValueType old_value;
  if (leaf->Lookup(key, &old_value, comparator_)) {
    return false;  // 存在重复key,插入失败
  }

  // 如果叶子节点已满,需要分裂
  if (leaf->GetSize() >= leaf_max_size_) {
    // 创建新的叶子节点
    auto new_guard = bpm_->NewPage();
    auto new_leaf = new_guard.AsMut<LeafPage>();
    new_leaf->Init(new_guard.PageId());
    
    // 分裂节点
    KeyType middle_key{};
    leaf->Split(middle_key, new_leaf);
    
    // 根据key的大小决定插入到哪个节点
    if (comparator_(key, middle_key) < 0) {
      leaf->Insert(key, value, comparator_);
    } else {
      new_leaf->Insert(key, value, comparator_);
    }
    
    // 更新父节点
    if (ctx.write_set_.size() > 1) {
      // 非根节点分裂,更新父节点
      auto *parent = ctx.write_set_[ctx.write_set_.size() - 2].AsMut<InternalPage>();
      parent->InsertNodeAfter(leaf->GetPageId(), middle_key, new_leaf->GetPageId());
    } else {
      // 根节点分裂,创建新的根节点
      auto new_root_guard = bpm_->NewPage();
      auto new_root = new_root_guard.AsMut<InternalPage>();
      new_root->Init(new_root_guard.PageId());
      new_root->PopulateNewRoot(leaf->GetPageId(), middle_key, new_leaf->GetPageId());
      
      // 更新header page中的root page id
      header_page->root_page_id_ = new_root_guard.PageId();
      ctx.root_page_id_ = new_root_guard.PageId();
    }
  } else {
    // 直接插入到叶子节点
    leaf->Insert(key, value, comparator_);
  }
  
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 *
 * @param key input key
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  // 检查树是否为空
  if (IsEmpty()) {
    return End();  // 空树返回end迭代器
  }

  // 创建上下文实例
  Context ctx;
  
  // 获取header page和root page id
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto *header_page = header_guard.As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  // 从根节点开始查找
  auto guard = bpm_->ReadPage(ctx.root_page_id_);
  auto *page = guard.As<BPlusTreePage>();
  ctx.read_set_.push_back(std::move(guard));

  // 一直往左子节点遍历直到叶子节点
  while (!page->IsLeafPage()) {
    auto *internal = ctx.read_set_.back().As<InternalPage>();
    // 获取最左边的子节点
    page_id_t next_page_id = internal->ValueAt(0);
    auto next_guard = bpm_->ReadPage(next_page_id);
    page = next_guard.As<BPlusTreePage>();
    ctx.read_set_.push_back(std::move(next_guard));
  }

  // 获取叶子节点
  auto *leaf = ctx.read_set_.back().As<LeafPage>();
  page_id_t leaf_page_id = ctx.read_set_.back().PageId();
  
  // 返回指向最左叶子节点第一个元素的迭代器
  return INDEXITERATOR_TYPE(bpm_, leaf_page_id, 0);

}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  // 检查树是否为空
  if (IsEmpty()) {
    return End();
  }

  // 创建上下文实例
  Context ctx;
  
  // 获取header page和root page id
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto *header_page = header_guard.As<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;

  // 从根节点开始查找
  auto guard = bpm_->ReadPage(ctx.root_page_id_);
  auto *page = guard.As<BPlusTreePage>();
  ctx.read_set_.push_back(std::move(guard));

  // 遍历到叶子节点
  while (!page->IsLeafPage()) {
    auto *internal = ctx.read_set_.back().As<InternalPage>();
    // 查找大于等于key的子节点
    page_id_t next_page_id = internal->Lookup(key, comparator_);
    auto next_guard = bpm_->ReadPage(next_page_id);
    page = next_guard.As<BPlusTreePage>();
    ctx.read_set_.push_back(std::move(next_guard));
  }

  // 获取叶子节点
  auto *leaf = ctx.read_set_.back().As<LeafPage>();
  page_id_t leaf_page_id = ctx.read_set_.back().PageId();
  
  // 在叶子节点中查找第一个大于等于key的位置
  int index = 0;
  while (index < leaf->GetSize() && comparator_(leaf->KeyAt(index), key) < 0) {
    index++;
  }
  
  // 返回指向找到位置的迭代器
  return INDEXITERATOR_TYPE(bpm_, leaf_page_id, index);

  }

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  return INDEXITERATOR_TYPE(bpm_, INVALID_PAGE_ID, 0);
}

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { 
  //UNIMPLEMENTED("TODO(P2): Add implementation."); 
  // 获取header page的读锁
  auto guard = bpm_->ReadPage(header_page_id_);
  
  // 从header page中获取root page id
  auto *header_page = guard.As<BPlusTreeHeaderPage>();
  
  // 返回root page id
  return header_page->root_page_id_;
  
  // guard会在函数返回时自动释放
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
