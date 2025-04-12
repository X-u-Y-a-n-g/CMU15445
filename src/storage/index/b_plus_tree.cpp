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
  // 获取头页面
  auto guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  
  // 如果根页面ID为INVALID_PAGE_ID，则树为空
  return root_page->root_page_id_ == INVALID_PAGE_ID;

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
  // 参数检查和初始化
  if (result == nullptr) {
    return false;
  }
  result->clear();
  
  if (IsEmpty()) {
    return false;
  }

  // 读取根页面
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  auto page_guard = bpm_->ReadPage(header_page->root_page_id_);
  auto tree_page = page_guard.As<BPlusTreePage>();

  // 遍历查找目标key
  while (!tree_page->IsLeafPage()) {
    auto internal_page = page_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    int index = 0;
    // 线性查找第一个大于等于key的位置
    while (index < internal_page->GetSize() - 1 && 
           comparator_(internal_page->KeyAt(index), key) < 0) {
      index++;
    }

    // 获取子节点页面
    page_id_t child_page_id = internal_page->ValueAt(index);
    page_guard = bpm_->ReadPage(child_page_id);
    tree_page = page_guard.As<BPlusTreePage>();
  }

  // 在叶子节点中查找
  auto leaf_page = page_guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  int index = 0;
  // 线性查找目标key
  while (index < leaf_page->GetSize() && 
         comparator_(leaf_page->KeyAt(index), key) < 0) {
    index++;
  }

  // 检查是否找到匹配的key
  if (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
    result->push_back(leaf_page->ValueAt(index));
    return true;
  }

  return false;
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
 auto BPLUSTREE_TYPE::SplitLeafPage(LeafPage *leaf, page_id_t leaf_page_id) -> std::pair<KeyType, page_id_t> {
   std::cout<<111;
  // 创建新叶子节点
   page_id_t new_page_id = bpm_->NewPage();
   auto new_node_guard = bpm_->WritePage(new_page_id);
   auto new_leaf = new_node_guard.template AsMut<LeafPage>();
   
   // 初始化新节点
   new_leaf->Init(leaf_max_size_);
   int size = leaf->GetSize();
   int mid = (size + 1) / 2;
   
   // 确保不会越界
   for (int i = 0; i < size - mid && i < leaf_max_size_; i++) {
     new_leaf->SetKeyAt(i, leaf->KeyAt(i + mid));
     new_leaf->SetValueAt(i, leaf->ValueAt(i + mid));
   }
   
   new_leaf->SetSize(size - mid);
   leaf->SetSize(mid);
   
   new_leaf->SetNextPageId(leaf->GetNextPageId());
   leaf->SetNextPageId(new_page_id);
   
   return {new_leaf->KeyAt(0), new_page_id};
 }
 
 INDEX_TEMPLATE_ARGUMENTS
 auto BPLUSTREE_TYPE::SplitInternalPage(InternalPage *internal, page_id_t internal_page_id) -> std::pair<KeyType, page_id_t> {
  std::cout<<222;
  // 创建新内部节点
   page_id_t new_page_id = bpm_->NewPage();
   auto new_node_guard = bpm_->WritePage(new_page_id);
   auto new_internal = new_node_guard.template AsMut<InternalPage>();
   
   // 初始化新节点
   new_internal->Init(internal_max_size_);
   int size = internal->GetSize();
  int mid = size / 2;
  KeyType middle_key = internal->KeyAt(mid);
  
  // 确保不会越界
  for (int i = 0; i < size - mid - 1 && i < internal_max_size_; i++) {
    new_internal->SetKeyAt(i, internal->KeyAt(i + mid + 1));
    new_internal->SetValueAt(i, internal->ValueAt(i + mid + 1));
    
    auto child_guard = bpm_->WritePage(internal->ValueAt(i + mid + 1));
    auto child = child_guard.template AsMut<BPlusTreePage>();
    child->SetParentPageId(new_page_id);
  }
  
  new_internal->SetSize(size - mid - 1);
  internal->SetSize(mid);
  
  return {middle_key, new_page_id};
 }
 
 // 原Split函数修改为根据节点类型调用对应的分裂函数
 INDEX_TEMPLATE_ARGUMENTS
template <typename T>
auto BPLUSTREE_TYPE::Split(T *node, page_id_t node_page_id) -> std::pair<KeyType, page_id_t> {
  if (node->IsLeafPage()) {
    return SplitLeafPage((LeafPage *)node, node_page_id);
  }
  return SplitInternalPage((InternalPage *)node, node_page_id);
}


// 处理向父节点插入
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *node, page_id_t node_page_id,
                                     const KeyType &key, page_id_t new_page_id) -> bool {
                                      std::cout<<333;
  page_id_t parent_page_id = node->GetParentPageId();
  auto parent_guard = bpm_->WritePage(parent_page_id);
  auto parent = parent_guard.template AsMut<InternalPage>();
  
  // 如果父节点未满，直接插入
  if (parent->GetSize() < parent->GetMaxSize()) {
    int index = parent->GetSize();
    // 确保不会越界
    for (int i = 0; i < parent->GetSize() && i < internal_max_size_; i++) {
      if (comparator_(parent->KeyAt(i), key) > 0) {
        index = i;
        break;
      }
    }
    
    // 移动元素时检查边界
    for (int i = parent->GetSize(); i > index && i < internal_max_size_; i--) {
      parent->SetKeyAt(i, parent->KeyAt(i - 1));
      parent->SetValueAt(i, parent->ValueAt(i - 1));
    }
    
    parent->SetKeyAt(index, key);
    parent->SetValueAt(index, new_page_id);
    parent->SetSize(parent->GetSize() + 1);
    
    auto new_node_guard = bpm_->WritePage(new_page_id);
    auto new_node = new_node_guard.template AsMut<BPlusTreePage>();
    new_node->SetParentPageId(parent_page_id);
    
    return true;
  }
  
  // 父节点已满，需要继续分裂
  auto [split_key, split_page_id] = Split(parent, parent_page_id);
  
  // 先插入当前的key和page_id
  if (comparator_(key, split_key) < 0) {
    // 插入到原节点
    int index = parent->GetSize();
    for (int i = 0; i < parent->GetSize(); i++) {
      if (comparator_(parent->KeyAt(i), key) > 0) {
        index = i;
        break;
      }
    }
    for (int i = parent->GetSize(); i > index; i--) {
      parent->SetKeyAt(i, parent->KeyAt(i - 1));
      parent->SetValueAt(i, parent->ValueAt(i - 1));
    }
    parent->SetKeyAt(index, key);
    parent->SetValueAt(index, new_page_id);
    parent->SetSize(parent->GetSize() + 1);
  } else {
    // 插入到新节点
    auto new_parent_guard = bpm_->WritePage(split_page_id);
    auto new_parent = new_parent_guard.template AsMut<InternalPage>();
    
    int index = new_parent->GetSize();
    for (int i = 0; i < new_parent->GetSize(); i++) {
      if (comparator_(new_parent->KeyAt(i), key) > 0) {
        index = i;
        break;
      }
    }
    for (int i = new_parent->GetSize(); i > index; i--) {
      new_parent->SetKeyAt(i, new_parent->KeyAt(i - 1));
      new_parent->SetValueAt(i, new_parent->ValueAt(i - 1));
    }
    new_parent->SetKeyAt(index, key);
    new_parent->SetValueAt(index, new_page_id);
    new_parent->SetSize(new_parent->GetSize() + 1);

    // 更新新节点的父指针
    auto new_node_guard = bpm_->WritePage(new_page_id);
    auto new_node = new_node_guard.AsMut<BPlusTreePage>();
    new_node->SetParentPageId(split_page_id);
  }
  
  // 递归向上处理父节点
  return InsertIntoParent(parent, parent_page_id, split_key, split_page_id);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  //UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but advised.
  Context ctx;

  // 获取头页面信息
  auto header_guard = bpm_->ReadPage(header_page_id_);
  auto header = header_guard.As<BPlusTreeHeaderPage>();

  // 如果树为空，创建新的根节点
  if (IsEmpty()) {
    std::cout<<444;
    page_id_t new_page_id = bpm_->NewPage();
    auto root_guard = bpm_->WritePage(new_page_id);
    auto root = root_guard.AsMut<LeafPage>();
    std::cout<<123;
    // 初始化根节点
    root->Init(leaf_max_size_);
    
    // 设置节点类型和父节点ID
    root->SetSize(0);  // 先设置大小为0
    root->SetPageType(IndexPageType::LEAF_PAGE);
    root->SetParentPageId(INVALID_PAGE_ID);
    root->SetNextPageId(INVALID_PAGE_ID);
    std::cout<<12345;
    // 插入第一个key-value对
    root->SetSize(1);
    root->SetKeyAt(0, key);
    root->SetValueAt(0, value);
    std::cout<<1234;
    // 更新头页面的根页面ID
    auto header_write_guard = bpm_->WritePage(header_page_id_);
    auto header = header_write_guard. template AsMut<BPlusTreeHeaderPage>();
    header->root_page_id_ = new_page_id;
    
    return true;
  }
  std::cout<<555;
  // 查找插入位置
  page_id_t current_page_id = header->root_page_id_;
  auto current_guard = bpm_->WritePage(current_page_id);
  auto current = current_guard.AsMut<BPlusTreePage>();

  // 遍历查找目标叶子节点
  while (!current->IsLeafPage()) {
    auto internal = current_guard.template AsMut<InternalPage>();
    int index = 0;
    while (index < internal->GetSize() && 
           comparator_(internal->KeyAt(index + 1), key) <= 0) {
      index++;
    }
    if (index > 0) {
      index--; // 回退一位，找到应该插入的子树
    }
    page_id_t next_page_id = internal->ValueAt(index);
    
    current_guard = bpm_->WritePage(next_page_id);
    current = current_guard.AsMut<BPlusTreePage>();
    current_page_id = next_page_id;
  }

  // 转换为叶子节点
  auto leaf = current_guard.AsMut<LeafPage>();

  // 检查重复键
  for (int i = 0; i < leaf->GetSize(); i++) {
    if (comparator_(leaf->KeyAt(i), key) == 0) {
      return false;
    }
  }

  // 如果节点未满，直接插入
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    std::cout<<666;
    // 找到插入位置
    int index = 0;
    // 找到第一个大于key的位置
    while (index < leaf->GetSize() && 
           comparator_(leaf->KeyAt(index), key) < 0) {
      index++;
    }
    // 移动元素，为新key腾出位置
    for (int i = leaf->GetSize(); i > index; i--) {
      leaf->SetKeyAt(i, leaf->KeyAt(i - 1));
      leaf->SetValueAt(i, leaf->ValueAt(i - 1));
    }
    // 插入新key-value
    leaf->SetKeyAt(index, key);
    leaf->SetValueAt(index, value);
    leaf->SetSize(leaf->GetSize() + 1);
    return true;
  }
  std::cout<<777;
  // 节点已满，需要分裂
  auto [split_key, new_page_id] = Split(leaf, current_page_id);

  // 把新key插入到合适的节点
  if (comparator_(key, split_key) < 0) {
    // 插入到原节点
    int index = 0;
    while (index < leaf->GetSize() && 
           comparator_(leaf->KeyAt(index), key) < 0) {
      index++;
    }

    for (int i = leaf->GetSize(); i > index; i--) {
      leaf->SetKeyAt(i, leaf->KeyAt(i - 1));
      leaf->SetValueAt(i, leaf->ValueAt(i - 1));
    }
    leaf->SetKeyAt(index, key);
    leaf->SetValueAt(index, value);
    leaf->SetSize(leaf->GetSize() + 1);
  } else {
    // 插入到新节点
    auto new_leaf_guard = bpm_->WritePage(new_page_id);
    auto new_leaf = new_leaf_guard.template AsMut<LeafPage>();
    int index = 0;
    while (index < new_leaf->GetSize() && 
           comparator_(new_leaf->KeyAt(index), key) < 0) {
      index++;
    }

    for (int i = new_leaf->GetSize(); i > index; i--) {
      new_leaf->SetKeyAt(i, new_leaf->KeyAt(i - 1));
      new_leaf->SetValueAt(i, new_leaf->ValueAt(i - 1));
    }
    new_leaf->SetKeyAt(index, key);
    new_leaf->SetValueAt(index, value);
    new_leaf->SetSize(new_leaf->GetSize() + 1);
  }
  std::cout<<888;
  // 处理分裂后的父节点更新
  if (leaf->GetParentPageId() == INVALID_PAGE_ID) {
    // 创建新的根节点
    page_id_t new_root_id = bpm_->NewPage();
    auto new_root_guard = bpm_->WritePage(new_root_id);
    auto new_root = new_root_guard.template AsMut<InternalPage>();
    
    new_root->Init(internal_max_size_);
    new_root->SetSize(2);
    new_root->SetValueAt(0, current_page_id);
    new_root->SetKeyAt(1, split_key);   // 第一个key是哨兵值
    new_root->SetValueAt(1, new_page_id);
    
    // 更新子节点的父指针
    leaf->SetParentPageId(new_root_id);
    auto new_node_guard = bpm_->WritePage(new_page_id);
    auto new_node = new_node_guard.template AsMut<BPlusTreePage>();
    new_node->SetParentPageId(new_root_id);
    
    // 更新根页面ID
    auto header_write_guard = bpm_->WritePage(header_page_id_);
    auto header = header_write_guard.AsMut<BPlusTreeHeaderPage>();
    header->root_page_id_ = new_root_id;
  } else {
    InsertIntoParent(leaf, current_page_id, split_key, new_page_id);
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
  UNIMPLEMENTED("TODO(P2): Add implementation.");
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { UNIMPLEMENTED("TODO(P2): Add implementation."); }

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub