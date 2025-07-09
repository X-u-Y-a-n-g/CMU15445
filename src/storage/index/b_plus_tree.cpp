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
  WritePageGuard guard = bpm_->WritePage(header_page_id_);  // 获取头页面的写保护
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();      // 获取头页面的指针
  root_page->root_page_id_ = INVALID_PAGE_ID;               // 设置根页面ID为无效页面ID
  // std::cout<< leaf_max_size << " - "<< internal_max_size <<std::endl;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
/**
 * @brief 辅助函数，用于判断当前B+树是否为空
 * @return 如果B+树没有键和值，则返回true
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);  // 获取头页面的写保护
  auto root_page = guard.As<BPlusTreeHeaderPage>();       // 获取头页面的指针
  return root_page->root_page_id_ == INVALID_PAGE_ID;     // 设置根页面ID为无效页面ID
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
/*****************************************************************************
 * 查找
 *****************************************************************************/
/**
 * @brief 返回与输入键关联的唯一值
 *
 * 此方法用于精确查询
 *
 * @param key 输入键
 * @param[out] result 如果值存在，则存储与输入键关联的唯一值的向量
 * @return : true表示键存在
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");

  // Declaration of context instance. Using the Context is not necessary but advised.
  if (IsEmpty()) {
    // 如果树为空树，直接返回false
    return false;
  }

  // 临时保存结果，避免直接修改输出参数
  // std::vector<ValueType> temp_result;

  // 清空结果向量，确保没有旧数据
  result->clear();
  // std::cout <<"finding "<< key << std::endl;
  Context ctx;
  ctx.root_page_id_ = GetRootPageId();
  ctx.header_page_ = bpm_->WritePage(header_page_id_);  // 头页面保护

  LeafPage *page = FindLeafPage(ctx, key, Operation::READ);
  // read 只有当前节点上了锁

  bool found = false;
  for (int i = 0; i < page->GetSize(); i++) {
    if (comparator_(key, page->KeyAt(i)) == 0) {
      result->push_back(page->ValueAt(i));
      found = true;
      return found;
      // 继续查找可能存在的多个匹配项，检测是否出现重复插入
    }
  }

  // 只有确认找到结果，才更新输出参数
  // if (found) {
  //   *result = std::move(temp_result);
  // }
  // 释放锁
  ctx.write_set_.clear();
  return found;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetBrother(Context &ctx, page_id_t cur_node_id, int &flag) -> page_id_t {
  if (ctx.write_set_.empty()) {
    return INVALID_PAGE_ID;
  }
  // 既然有孩子节点，那就说明父节点一定是内部节点 ，于是
  auto parent_ptr = ctx.write_set_.back().AsMut<InternalPage>();  // father ptr

  if (parent_ptr == nullptr) {
    return INVALID_PAGE_ID;
  }

  if (ctx.write_set_.back().GetPageId() == INVALID_PAGE_ID) {
    // 如果是无效id 说明为空，没有兄弟节点
    return INVALID_PAGE_ID;
  }

  // 当前节点的在parent的 index
  int index_page = parent_ptr->ValueIndex(cur_node_id);

  // 如果是最后一个节点，就返回左孩子节点，其他的就返回右孩子节点；
  if (index_page == parent_ptr->GetSize() - 1) {
    flag = 1;  // 左兄弟
    return parent_ptr->ValueAt(index_page - 1);
  }
  flag = 2;  // 右兄弟
  return parent_ptr->ValueAt(index_page + 1);
}
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::leafSplit(page_id_t &old_node, page_id_t &new_node) -> KeyType {
//   auto old_guard = bpm_->WritePage(old_node);  //旧节点的写锁
//   auto new_guard = bpm_->WritePage(new_node);  //新节点的写锁
//   //获取新旧节点的指针
//   LeafPage *old_ptr = old_guard.AsMut<LeafPage>();
//   LeafPage *new_ptr = new_guard.AsMut<LeafPage>();
//   int old_max_size = old_ptr->GetMaxSize();
//   //从中间，也就是minsize - 1 的位置将 值给新节点
//   int j = 0;
//   for (int i = old_ptr->GetMinSize(); i < old_max_size; i++, j++) {
//     new_ptr->SetKeyAt(j, old_ptr->KeyAt(i));
//     new_ptr->SetValueAt(j, old_ptr->ValueAt(i));
//   }
//   new_ptr->ChangeSizeBy(old_max_size - old_ptr->GetMinSize());  //新节点中的数量就是转移的节点的数量
//   old_ptr->SetSize(old_ptr->GetMinSize());                      //将节点数量更新
//   return new_ptr->KeyAt(0);                                     //返回首节点的位置
// }

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::leafDelete(page_id_t &node, KeyType &key) -> bool {
//   auto node_guard = bpm_->WritePage(node);            //获取写锁
//   LeafPage *node_ptr = node_guard.AsMut<LeafPage>();  //获取指针
//   bool found = false;
//   int i = 0;
//   int node_size = node_ptr->GetSize();
//   for (; i < node_size; i++) {
//     if (comparator_(node_ptr->KeyAt(i), key)) {
//       found = true;
//       break;
//     }
//   }
//   if (!found) return false;
//   int j = i;
//   // 将节点 进行删除

//   //前移
//   for (; j < node_size - 1; j++) {
//     node_ptr->SetKeyAt(j, node_ptr->KeyAt(j + 1));
//     node_ptr->SetValueAt(j, node_ptr->ValueAt(j + 1));
//   }
//   node_ptr->ChangeSizeBy(-1);
//   return true;
// }

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::leafInsert(page_id_t &node, KeyType key, ValueType newpage) -> bool {
//   auto node_guard = bpm_->WritePage(node);
//   LeafPage *node_ptr = node_guard.AsMut<LeafPage>();
//   int i = 0;
//   for (; i < node_ptr->GetSize() && comparator_(key, node_ptr->KeyAt(i)) > 0; i++) {
//   }
//   for (int j = node_ptr->GetSize() - 1; j > i; j--) {
//     node_ptr->SetKeyAt(j, node_ptr->KeyAt(j - 1));
//     node_ptr->SetValueAt(j, node_ptr->ValueAt(j - 1));
//   }
//   node_ptr->SetKeyAt(i, key);
//   node_ptr->SetValueAt(i, newpage);
//   node_ptr->ChangeSizeBy(1);
//   return true;
// }

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::leafMerge(page_id_t &old_node, page_id_t &new_node) -> bool {
//   // get ptr
//   auto old_guard = bpm_->WritePage(old_node);
//   auto new_guard = bpm_->WritePage(new_node);
//   LeafPage *old_ptr = old_guard.AsMut<LeafPage>();
//   LeafPage *new_ptr = new_guard.AsMut<LeafPage>();

//   if (new_ptr->GetSize() + old_ptr->GetSize() > old_ptr->GetMaxSize()) {
//     return false;
//   }
//   for (int i = old_ptr->GetSize(); i < new_ptr->GetSize(); i++) {
//     leafInsert(old_node, new_ptr->KeyAt(i), new_ptr->ValueAt(i));
//   }
//   return true;
// }

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::interMerge(page_id_t &old_node, page_id_t &new_node) -> bool {
//   // 内部节点的合并，将所有的newnode添加到 old弄的上面
//   auto old_guard = bpm_->WritePage(old_node);
//   auto new_guard = bpm_->WritePage(new_node);
//   InternalPage *old_ptr = old_guard.AsMut<InternalPage>();
//   InternalPage *new_ptr = new_guard.AsMut<InternalPage>();
//   //将new节点的第一个节点的元素拿出来
//   auto new_first_value = new_ptr->ValueAt(0);
//   auto new_first_guard = bpm_->WritePage(new_first_value);
//   //判断一下是什么节点，如果是叶子节点，那就是去index为0的值，如果是内部节点，就去找 index为1的元素的值
//   BPlusTreePage *new_first_ptr = new_first_guard.template AsMut<BPlusTreePage>();
//   KeyType key;
//   if (new_first_ptr->IsLeafPage()) {
//     auto newptr = new_first_guard.template As<LeafPage>();
//     key = newptr->KeyAt(0);
//   } else {
//     auto newptr = new_first_guard.template As<InternalPage>();
//     key = newptr->KeyAt(1);
//   }
//   //有了第一个元素之后，我们可以对节点进行合并了
//   old_ptr->SetKeyAt(old_ptr->GetSize(), key);
//   old_ptr->ChangeSizeBy(1);
//   old_ptr->SetValueAt(old_ptr->GetSize(), new_ptr->ValueAt(0));
//   for (int i = 1; i < new_ptr->GetSize(); i++) {
//     old_ptr->SetKeyAt(old_ptr->GetSize(), new_ptr->KeyAt(i));
//     old_ptr->ChangeSizeBy(1);
//     old_ptr->SetValueAt(old_ptr->GetSize(), new_ptr->ValueAt(i));
//   }
//   return true;
//   //这样就将节点合并完成，但是只是对节点进行简单的合并，并没有改变任何的属性
// }

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::interInsert( page_id_t &node , KeyType key , ValueType value) ->bool{
//   auto guard = bpm_->WritePage(node);
//   InternalPage* ptr = guard.AsMut<InternalPage>();
//   int i = 1;
//   for(; i<ptr->GetSize() ;i++){
//     if(comparator_(ptr->KeyAt(i),key) > 0) break;
//   }
//   for(int j = ptr->GetSize() ; j > i + 1 ;j--){
//     ptr->SetKeyAt(j , ptr->KeyAt(j - 1));
//     ptr->SetValueAt(j , ptr->ValueAt(j -1));
//   }
//   //然后在位置 i 插入 key-value
//   ptr->SetKeyAt( i , key);
//   ptr->SetValueAt(i,value);
//   ptr->ChangeSizeBy(1);
//   return true;
// }

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::interDelete(page_id_t &node, KeyType key) -> bool {
//   auto guard = bpm_->WritePage(node);
//   InternalPage *ptr = guard.AsMut<InternalPage>();
//   int i = 0;
//   bool found = false;
//   for (; i < ptr->GetSize(); i++) {
//     if (comparator_(key, ptr->KeyAt(i)) == 0) {
//       found = true;
//       break;
//     }
//   }
//   if (!found) return false;

//   return true;
// }

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
/*****************************************************************************
 * 插入
 *****************************************************************************/
/**
 * @brief 将常量键值对插入B+树
 *
 * 如果当前树为空，则创建新树，更新根页面ID并插入
 * 条目，否则插入叶子页面。
 *
 * @param key 要插入的键
 * @param value 与键关联的值
 * @return: 由于我们只支持唯一键，如果用户尝试插入重复的
 * 键则返回false，否则返回true。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  // std::cout << key <<" - "<< value << std::endl;

  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);  // 对头页面进行加锁
  auto header = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  // 如果是 空节点 就创建一个节点 更新属性
  if (header->root_page_id_ == INVALID_PAGE_ID) {
    //   如果为空，就在申请一个新的空页面 来作 为根 节点, 而且这个页面一定是一个叶子页面
    ctx.root_page_id_ = bpm_->NewPage();
    if (ctx.root_page_id_ == INVALID_PAGE_ID) {
      return false;
    }
    header->root_page_id_ = ctx.root_page_id_;

    WritePageGuard root_guard = bpm_->WritePage(ctx.root_page_id_);
    auto *new_root_ptr = root_guard.AsMut<LeafPage>();

    new_root_ptr->Init(leaf_max_size_);
    new_root_ptr->SetPageType(IndexPageType::LEAF_PAGE);
    // new_root_ptr->SetPageId(ctx.root_page_id_);
    new_root_ptr->SetKeyAt(0, key);
    new_root_ptr->SetValueAt(0, value);
    new_root_ptr->SetSize(1);

    return true;  //  插入完成
  }

  // 非空树的情况
  ctx.root_page_id_ = header->root_page_id_;

  // 螃蟹法 查找插入的叶节点所在的位置 , 如果为空 直接返回
  LeafPage *leaf_page = FindLeafPage(ctx, key, Operation::INSERT);

  // 存疑？ 会出现空的情况吗？
  if (leaf_page == nullptr) {
    return false;
  }

  for (int i = 0; i < leaf_page->GetSize(); i++) {
    // 如果找到相同的键，返回失败（不支持重复键）
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      return false;
    }
  }

  // 如果叶子节点没有满，直接进行插入操作
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    int i = 0;  // 寻找插入的合适的位置
    while (i < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(i), key) < 0) {
      i++;
    }

    // 放心插入，前面已经判断过有剩余的位置
    for (int j = leaf_page->GetSize() - 1; j >= i; j--) {
      leaf_page->SetValueAt(j + 1, leaf_page->ValueAt(j));
      leaf_page->SetKeyAt(j + 1, leaf_page->KeyAt(j));
    }
    // 插入 新节点
    leaf_page->SetKeyAt(i, key);
    leaf_page->SetValueAt(i, value);
    leaf_page->ChangeSizeBy(1);
    return true;
  }

  // 叶子节点满了，就需要进行叶节点的 分裂
  page_id_t new_leaf_id = bpm_->NewPage();
  if (new_leaf_id == INVALID_PAGE_ID) {
    return false;  // 说明没有分配到新页面 插入失败
  }

  // 获取新叶子节点的锁和指针
  auto new_leaf_guard = bpm_->WritePage(new_leaf_id);
  auto new_leaf = new_leaf_guard.template AsMut<LeafPage>();
  // 初始化新的叶子节点 新叶子节点的父节点一定是 当前节点的父节点 ,如果有需要改的地方 会进行更新的
  new_leaf->SetPageType(IndexPageType::LEAF_PAGE);
  new_leaf->Init(leaf_max_size_);
  // new_leaf->SetPageId(new_leaf_id);
  // new_leaf->SetParentPageId(leaf_page->GetParentPageId());

  // 接下来就开始进行叶子节点的分裂
  std::vector<KeyType> keys;
  std::vector<ValueType> values;
  // 将所有的元素先放入容器内部 ，以便进行后面的重新分配
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    keys.push_back(leaf_page->KeyAt(i));
    values.push_back(leaf_page->ValueAt(i));
  }

  // 查找应该插入的位置，和上面的方法一样
  int insert_pos = 0;
  while (insert_pos < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(insert_pos), key) < 0) {
    insert_pos++;
  }
  // 在这个位置进行插入
  keys.insert(keys.begin() + insert_pos, key);
  values.insert(values.begin() + insert_pos, value);
  int num = leaf_page->GetSize() + 1;
  //  然后对节点进行重新分配
  int i = 0;
  // 将前 GetMinSize() 个节点还分给 当前节点 ，剩余的分给新的节点
  for (; i < leaf_page->GetMinSize(); i++) {
    leaf_page->SetKeyAt(i, keys[i]);
    leaf_page->SetValueAt(i, values[i]);
  }

  for (int j = 0; i < num; i++, j++) {
    new_leaf->SetKeyAt(j, keys[i]);
    new_leaf->SetValueAt(j, values[i]);
  }
  // 重新设置大小，当前节点数量为 最小值， 新节点的数量为 总数 - 最小值
  leaf_page->SetSize(leaf_page->GetMinSize());
  new_leaf->SetSize(num - leaf_page->GetSize());

  // 维护叶子节点之间的链表， 将新叶子节点插在 当前节点的后面
  new_leaf->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_id);

  // 将新叶子节点的最小key(获取)  交给父节点 释放叶子节点的锁
  KeyType middle_key = new_leaf->KeyAt(0);
  page_id_t leaf_page_id = ctx.write_set_.back().GetPageId();
  new_leaf_guard = WritePageGuard{};
  ctx.write_set_.pop_back();

  keys.clear();          // 移除所有元素
  keys.shrink_to_fit();  // 请求释放未使用的内存
  values.clear();
  values.shrink_to_fit();

  InsertIntoParent(ctx, leaf_page_id, middle_key, new_leaf_id);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(Context &ctx, const KeyType &key, Operation op) -> LeafPage * {
  // 获取根页面
  page_id_t page_id = ctx.root_page_id_;
  // 然后从根开始遍历
  BPlusTreePage *page = nullptr;

  auto guard = bpm_->WritePage(page_id);
  ctx.write_set_.push_back(std::move(guard));
  page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
  // std::cout <<" root- id" <<page->GetPageId()<<std::endl;

  while (!page->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    // std::cout <<" key - first" <<internal->KeyAt(1)<<std::endl;
    //  寻找子节点的位置

    int index = internal->GetSize() - 1;
    for (int i = 1; i < internal->GetSize(); i++) {
      if (comparator_(key, internal->KeyAt(i)) < 0) {
        break;
      }
      index = i;
    }

    if (comparator_(key, internal->KeyAt(1)) < 0) {
      index = 0;
    }
    // 获取子节点的pageid
    page_id_t child_page_id = internal->ValueAt(index);

    // 获取对应的 指针, 锁进入队列
    auto guard = bpm_->WritePage(child_page_id);
    page = guard.template AsMut<BPlusTreePage>();

    //   if(!ctx.write_set_.empty()){
    //   const InternalPage* parent = ctx.write_set_.back().As<InternalPage>();
    //   auto parent_size = parent->GetSize();
    //   auto parent_max_size = parent->GetMaxSize();
    //   auto parent_min_size = parent->GetMinSize();
    //   //------------------------------------------------- 这里使用螃蟹法为什么更慢？
    //   if(op == Operation::DELETE &&parent_size > parent_min_size)
    //   {
    //     ctx.header_page_ = WritePageGuard{};
    //     ctx.write_set_.clear();
    //   }else if(op == Operation::INSERT && parent_size < parent_max_size ){
    //     ctx.header_page_ = WritePageGuard{};
    //     ctx.write_set_.clear();
    //   }else if(op == Operation::READ){
    //     ctx.header_page_ = WritePageGuard{};
    //     ctx.write_set_.clear();
    //   }
    // }
    ctx.write_set_.push_back(std::move(guard));
  }
  // 这样就直接找到了对应的叶子节点
  auto p = reinterpret_cast<LeafPage *>(page);
  // std:: cout << " p " <<p->KeyAt(0) << std::endl;
  return p;
}

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::FindLeafPage(Context &ctx, const KeyType &key, Operation op) -> LeafPage * {
//   // 获取根页面
//   page_id_t page_id = ctx.root_page_id_;
//   // 然后从根开始遍历
//   BPlusTreePage *page = nullptr;

//   auto guard = bpm_->WritePage(page_id);
//   ctx.write_set_.push_back(std::move(guard));
//   page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
//   // std::cout <<" root- id" <<page->GetPageId()<<std::endl;

//   while (!page->IsLeafPage()) {
//     auto *internal = reinterpret_cast<InternalPage *>(page);
//     // std::cout <<" key - first" <<internal->KeyAt(1)<<std::endl;
//     //  寻找子节点的位置

//     int index = internal->GetSize() - 1;
//     for (int i = 1; i < internal->GetSize(); i++) {
//       if (comparator_(key, internal->KeyAt(i)) < 0) {
//         break;
//       }
//       index = i;
//     }

//     if (comparator_(key, internal->KeyAt(1)) < 0) {
//       index = 0;
//     }
//     // 获取子节点的pageid
//     page_id_t child_page_id = internal->ValueAt(index);

//     // 获取对应的 指针, 锁进入队列

//     auto guard = bpm_->WritePage(child_page_id);
//     page = guard.template AsMut<BPlusTreePage>();
//     if(!ctx.write_set_.empty()){

//     const InternalPage* parent = ctx.write_set_.back().As<InternalPage>();
//     auto parent_size = parent->GetSize();
//     auto parent_max_size = parent->GetMaxSize();
//     auto parent_min_size = parent->GetMinSize();
//     // ------------------------------------------------- 这里使用螃蟹法为什么更慢？
//     if(op == Operation::DELETE &&parent_size > parent_min_size)
//     {
//       ctx.header_page_ = WritePageGuard{};
//       ctx.write_set_.clear();
//     }else if(op == Operation::INSERT && parent_size < parent_max_size ){
//       ctx.header_page_ = WritePageGuard{};
//       ctx.write_set_.clear();
//     }else if(op == Operation::READ){
//       ctx.header_page_ = WritePageGuard{};
//       ctx.write_set_.clear();
//     }
//   }
//     ctx.write_set_.push_back(std::move(guard));
//   }
//   // 这样就直接找到了对应的叶子节点
//   auto p = reinterpret_cast<LeafPage *>(page);
//   // std:: cout << " p " <<p->KeyAt(0) << std::endl;
//   return p;
// }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(Context &ctx, page_id_t left_page_id, const KeyType &middle_key,
                                      page_id_t right_page_id) -> bool {
  auto left_page_guard = bpm_->WritePage(left_page_id);
  // BPlusTreePage *left_page = left_page_guard.AsMut<BPlusTreePage>();
  auto right_guard = bpm_->WritePage(right_page_id);
  // BPlusTreePage *right_page = right_guard.AsMut<BPlusTreePage>();

  // 如果 旧的节点是根页面 ，创建新的根界面， 将两个节点插入
  if (ctx.IsRootPage(left_page_guard.GetPageId())) {
    // 生成新的根节点
    page_id_t new_root_id = bpm_->NewPage();
    if (new_root_id == INVALID_PAGE_ID) {
      return false;
    }
    // 获得 新的根节点的锁 ，指针
    auto new_root_guard = bpm_->WritePage(new_root_id);
    auto new_root = new_root_guard.AsMut<InternalPage>();

    // 初始化根节点
    new_root->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_root->Init(internal_max_size_);
    // new_root->SetPageId(new_root_id);
    new_root->SetValueAt(0, left_page_id);
    new_root->SetKeyAt(1, middle_key);
    new_root->SetValueAt(1, right_page_id);
    new_root->SetSize(2);

    // 更新父节点
    // right_page->SetParentPageId(new_root_id);
    // left_page->SetParentPageId(new_root_id);

    // 释放左右孩子节点的 锁
    left_page_guard = WritePageGuard{};
    right_guard = WritePageGuard{};
    // 释放 根节点的锁
    new_root_guard = WritePageGuard{};

    // 更新树的头 页面 中 储存的 根页面 的id
    auto header = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
    header->root_page_id_ = new_root_id;
    ctx.root_page_id_ = new_root_id;  // 其实没必要
    return true;
  }

  // 首先判断当前节点是否可以直接插入，因为根据螃蟹法 ，如果可以直接插入 就不用进行迭代
  // 根据我们的逻辑 如果进入到这一层 说明 这一层里面 ctx.write_set_是保存有当前节点的父节点锁的

  // 如果不是跟页面，那我们就进行迭代插入
  auto parent = ctx.write_set_.back().AsMut<InternalPage>();

  // 更新右页面的父指针
  // right_page->SetParentPageId(parent->GetPageId());

  // 如果父页面未满，直接插入
  if (parent->GetSize() < parent->GetMaxSize()) {
    //  找到插入位置，既然是右孩子页面 就不用考虑在第一个位置上，因为永远不可能在第一个位置上（索引为0 ）的位置
    int insert_index = parent->GetSize();  // 默认插入到末尾，
    for (int i = 1; i < parent->GetSize(); i++) {
      if (comparator_(middle_key, parent->KeyAt(i)) < 0) {
        //  找到比要插入的元素刚好大的位置，进行插入
        insert_index = i;
        break;
      }
    }

    // 移动元素腾出插入位置
    for (int i = parent->GetSize(); i > insert_index; i--) {
      parent->SetKeyAt(i, parent->KeyAt(i - 1));
      parent->SetValueAt(i, parent->ValueAt(i - 1));
    }

    // 插入新键值对
    parent->SetKeyAt(insert_index, middle_key);
    parent->SetValueAt(insert_index, right_page_id);
    parent->ChangeSizeBy(1);
    return true;
  }

  // 父页面已满，需要分裂
  page_id_t new_internal_id = bpm_->NewPage();
  // std::cout<<"需要分裂"<< std::endl;
  if (new_internal_id == INVALID_PAGE_ID) {
    return false;  // 页面分配失败
  }
  auto new_internal_guard = bpm_->WritePage(new_internal_id);
  auto new_internal = new_internal_guard.AsMut<InternalPage>();
  new_internal->Init(internal_max_size_);
  // new_internal->SetPageId(new_internal_id);
  // new_internal->SetParentPageId(parent->entPageId());
  new_internal->SetPageType(IndexPageType::INTERNAL_PAGE);

  // 找到新键应该插入的位置
  int insert_index = parent->GetSize();  //  默认位置在最后
  for (int i = 1; i < parent->GetSize(); i++) {
    if (comparator_(middle_key, parent->KeyAt(i)) < 0) {
      insert_index = i;
      break;
    }
  }

  // 不可能插入在 0 的位置 ，因为根据我们的逻辑 ，我们分裂为的是 柚子树 ，他的索引一定要比左树的大
  if (insert_index == 0) {
    throw Exception("b_pluss_tree.cpp 616 错误索引");
  }
  // 接下来就是对键值对的更新，进行重新的分配
  // 我们可以先保存所有的 键值对 ，先进行插入 ，后进行分配 ，和叶子节点不同，这里少了一一个key，需要注意这一点
  // 临时存储原始键值对加上新插入的键值对
  std::vector<KeyType> keys;
  std::vector<page_id_t> values;

  // 储存所有现有键值对
  values.push_back(parent->ValueAt(0));
  for (int i = 1; i < parent->GetSize(); i++) {
    keys.push_back(parent->KeyAt(i));
    values.push_back(parent->ValueAt(i));
  }

  // insert_index至少为1；
  keys.insert(keys.begin() + insert_index - 1, middle_key);
  values.insert(values.begin() + insert_index, right_page_id);

  // 确定现在的总元素个数与mid的位置
  int num = parent->GetSize() + 1;
  int mid = parent->GetMinSize();

  // 对原来的节点进行重新分配
  parent->SetValueAt(0, values[0]);
  for (int i = 1; i < mid; i++) {
    parent->SetKeyAt(i, keys[i - 1]);
    parent->SetValueAt(i, values[i]);
  }
  KeyType internal_middle_key = keys[mid - 1];  // 中间的键值对
  // 对新节点进行填充
  new_internal->SetValueAt(0, values[mid]);
  for (int i = mid + 1, j = 1; i < num; i++, j++) {
    new_internal->SetKeyAt(j, keys[i - 1]);
    new_internal->SetValueAt(j, values[i]);
  }
  // 对新节点进行数量的设置
  new_internal->SetSize(num - mid);
  parent->SetSize(mid);
  // new_internal->SetParentPageId(parent->GetParentPageId());

  // 这时候左右孩子节点都已经不需要用了，使用新的锁之前 先置为空
  // 递归向上插入中间键
  page_id_t parent_page_id = ctx.write_set_.back().GetPageId();
  left_page_guard = WritePageGuard{};
  right_guard = WritePageGuard{};
  ///////////// 真的不想维护了 以后有机会将parent属性删除
  // 维护孩子节点的父亲属性
  // for(int i = 0 ;i < parent->GetSize(); i++){
  //   page_id_t child_id = parent->ValueAt(i);
  //   auto guard = bpm_->WritePage(child_id);
  //   BPlusTreePage* c = guard.AsMut<BPlusTreePage>();
  //   //c->SetParentPageId(parent->GetPageId());
  //   // if(c->IsLeafPage()){
  //   //   LeafPage* l = guard.AsMut<LeafPage>();
  //   //   std::cout<< "child "<<i <<" first -key" << l->KeyAt(1) <<std::endl;
  //   // }else{
  //   //   InternalPage* in = guard.AsMut<InternalPage>();
  //   //   std::cout<<  " child " <<i <<"first -key" << in->KeyAt(1)<<std::endl;
  //   // }
  //   guard = WritePageGuard{};
  // }
  // for(int i = 0 ;i < new_internal->GetSize(); i++){
  //   page_id_t child_id = new_internal->ValueAt(i);
  //   auto guard = bpm_->WritePage(child_id);
  //   BPlusTreePage* c = guard.AsMut<BPlusTreePage>();
  //   // if(c->IsLeafPage()){
  //   //   LeafPage* l = guard.AsMut<LeafPage>();
  //   //  // std::cout<< "child "<<i <<" first -key" << l->KeyAt(1) <<std::endl;
  //   // }else{
  //   //   InternalPage* in = guard.AsMut<InternalPage>();
  //   //  // std::cout<<  " child " <<i <<"first -key" << in->KeyAt(1)<<std::endl;
  //   // }
  //  // c->SetParentPageId(new_internal->GetPageId());
  //   guard = WritePageGuard{};
  // }
  // 释放 空间
  keys.clear();
  keys.shrink_to_fit();
  values.clear();
  values.shrink_to_fit();

  // 释放所有的锁
  new_internal_guard = WritePageGuard{};
  ctx.write_set_.pop_back();

  return InsertIntoParent(ctx, parent_page_id, internal_middle_key, new_internal_id);
}

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::FindMinimumKey(page_id_t page_id) -> KeyType {
//   // 获取页面
//   auto page_guard = bpm_->WritePage(page_id);
//   auto page = page_guard.AsMut<BPlusTreePage>();

//   if (page == nullptr) {
//     std::cout << "叶子节点不存在" << std::endl;
//   }
//   // 如果是叶子节点，返回第一个键
//   if (page->IsLeafPage()) {
//     auto leaf = reinterpret_cast<LeafPage *>(page);
//     // 确保叶子节点不为空
//     if (leaf->GetSize() > 0) {
//       return leaf->KeyAt(0);
//     }
//     std::cout << "空叶子节点" << std::endl;
//   }

//   // 如果是内部节点，沿着最左侧指针向下查找
//   auto internal = reinterpret_cast<InternalPage *>(page);
//   if (internal->GetSize() > 0) {
//     // 内部节点的第一个值指向最左侧子节点
//     page_id_t child_page_id = internal->ValueAt(0);
//     // 递归查找
//     return FindMinimumKey(child_page_id);
//   }
//   std::cout << "空内部节点" << std::endl;
//   // throw Exception("空内部节点");
//   return KeyType();
// }

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
/*****************************************************************************
 * 删除
 *****************************************************************************/
/**
 * @brief 删除与输入键关联的键值对
 * 如果当前树为空，则立即返回。
 * 如果不为空，用户需要首先找到正确的叶子页面作为删除目标，然后
 * 从叶子页面删除条目。记得在必要时处理重新分配或合并。
 *
 * @param key 输入键
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  // 如果树为空 ，直接返回
  if (IsEmpty()) {
    return;
  }
  Context ctx;
  // 对ctx进行初始化
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto *header = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;

  // 找到包含键的叶子节点 ，并对沿途的节点加上锁 ， 包括新的叶子节点
  LeafPage *leaf_page = FindLeafPage(ctx, key, Operation::DELETE);
  // 接下来是删除叶子节点之中的key
  // if(key.ToString() == 917){
  //   std::cout<<"901"<<std::endl;
  // }
  if (leaf_page == nullptr) {
    return;
  }
  // 从容器末尾获取元素并转移所有权
  auto leaf_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();  // 移除已转移的元素

  // 检查键是否存在于叶子节点
  int delete_index = -1;
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      delete_index = i;
      break;
    }
  }
  // 如果键不存在 ，说明要删除的元素不在 ，直接返回
  if (delete_index == -1) {
    return;
  }

  // 从叶子节点中删除 键值对
  for (int i = delete_index; i < leaf_page->GetSize() - 1; i++) {
    leaf_page->SetKeyAt(i, leaf_page->KeyAt(i + 1));
    leaf_page->SetValueAt(i, leaf_page->ValueAt(i + 1));
  }
  // 考虑一下怎么把value释放掉--------------------------------------------
  leaf_page->ChangeSizeBy(-1);

  // 检查是否为根节点
  if (ctx.IsRootPage(leaf_guard.GetPageId())) {
    // 如果根节点为空 ，直接删除根节点就好
    if (leaf_page->GetSize() == 0) {
      ctx.write_set_.clear();
      bpm_->DeletePage(ctx.root_page_id_);
      header->root_page_id_ = INVALID_PAGE_ID;
    }
    // 如果不为空 直接完成
    return;
  }

  // 如果节点大于等于 最小限制
  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    return;
  }
  // 如果小于 意味着我们需要进行 重新分配 者合并或

  // 获取兄弟节点
  int brother_flag = 0;  // 1表示左兄弟，2表示右兄弟

  // 传入节点需要上锁 ，ctx内为父节点的锁
  page_id_t brother_page_id = GetBrother(ctx, leaf_guard.GetPageId(), brother_flag);

  // 父节点有效一定有兄弟节点
  if (brother_page_id == INVALID_PAGE_ID) {
    return;
  }
  // 获取兄弟节点的锁 和 指针
  auto brother_guard = bpm_->WritePage(brother_page_id);
  auto brother_page = brother_guard.AsMut<LeafPage>();
  // 获取父节点的指针
  auto parent = ctx.write_set_.back().AsMut<InternalPage>();

  // 准备进行合并或者分配
  // 合并的条件是 两个节点家在一起比最大值小；
  if (leaf_page->GetSize() + brother_page->GetSize() <= leaf_page->GetMaxSize()) {
    // 开始合并，都向左边移动
    if (brother_flag == 1) {  // 左兄弟
      // 将当前节点合并到左兄弟上面 ，对于叶子节点来说 ，直接移动即可
      for (int i = 0; i < leaf_page->GetSize(); i++) {
        brother_page->SetKeyAt(brother_page->GetSize() + i, leaf_page->KeyAt(i));
        brother_page->SetValueAt(brother_page->GetSize() + i, leaf_page->ValueAt(i));
      }
      brother_page->ChangeSizeBy(leaf_page->GetSize());

      // 更新链表指针
      brother_page->SetNextPageId(leaf_page->GetNextPageId());

      // 在父节点中 删除当前节点（右侧）的键值对，因为是要删去右边的孩子，
      // 所以不用考虑会删掉第一个节点
      int parent_index = parent->ValueIndex(leaf_guard.GetPageId());

      // 将所在的节点进行覆盖
      for (int i = parent_index; i < parent->GetSize() - 1; i++) {
        parent->SetKeyAt(i, parent->KeyAt(i + 1));
        parent->SetValueAt(i, parent->ValueAt(i + 1));
      }
      parent->ChangeSizeBy(-1);

      // 释放当前节点
      page_id_t leaf_id = leaf_guard.GetPageId();
      // 释放两兄弟锁
      leaf_guard = WritePageGuard{};
      brother_guard = WritePageGuard{};
      bpm_->DeletePage(leaf_id);

      // 对父节点进行重新分配或者合并
      page_id_t parent_id = ctx.write_set_.back().GetPageId();

      ctx.write_set_.pop_back();  // 将父节点的保护弹 出去
      HandleParentUnderflow(ctx, parent_id);
    } else {  //  右兄弟 和左兄弟类似
      //  将右边的兄弟合并到当前节点
      for (int i = 0; i < brother_page->GetSize(); i++) {
        leaf_page->SetKeyAt(leaf_page->GetSize() + i, brother_page->KeyAt(i));
        leaf_page->SetValueAt(leaf_page->GetSize() + i, brother_page->ValueAt(i));
      }
      leaf_page->ChangeSizeBy(brother_page->GetSize());

      // 更新列表的指针
      leaf_page->SetNextPageId(brother_page->GetNextPageId());

      // 在父节点中找到右兄弟的位置
      int parent_index = parent->ValueIndex(brother_page_id);

      // 从父节点删除（覆盖）右兄弟的键值对
      for (int i = parent_index; i < parent->GetSize() - 1; i++) {
        parent->SetKeyAt(i, parent->KeyAt(i + 1));
        parent->SetValueAt(i, parent->ValueAt(i + 1));
      }
      parent->ChangeSizeBy(-1);
      leaf_guard = WritePageGuard{};
      brother_guard = WritePageGuard{};

      // 删除这个叶子节点
      bpm_->DeletePage(brother_page_id);

      // 找到这个 有元素被删除的内部节点，去递归处理（已经删除）
      page_id_t parent_id = ctx.write_set_.back().GetPageId();
      // 释放锁
      ctx.write_set_.pop_back();

      // 检查父节点是否需要重新分配或合并
      HandleParentUnderflow(ctx, parent_id);
    }
  } else {
    // 如果 不能合并，那就需要进行重新分配，也就是从兄弟借元素
    if (brother_flag == 1) {  // 左兄弟
      // 为当前节点腾出空间
      for (int i = leaf_page->GetSize(); i > 0; i--) {
        leaf_page->SetKeyAt(i, leaf_page->KeyAt(i - 1));
        leaf_page->SetValueAt(i, leaf_page->ValueAt(i - 1));
      }
      // 移动左兄弟最后一个元素到当前节点，当前处理的是叶子节点
      leaf_page->SetKeyAt(0, brother_page->KeyAt(brother_page->GetSize() - 1));
      leaf_page->SetValueAt(0, brother_page->ValueAt(brother_page->GetSize() - 1));
      brother_page->ChangeSizeBy(-1);
      leaf_page->ChangeSizeBy(1);

      // 更新父节点中的键
      int parent_index = parent->ValueIndex(leaf_guard.GetPageId());
      parent->SetKeyAt(parent_index, leaf_page->KeyAt(0));

    } else {  // 从右兄弟借一个元素
      // 移动右兄弟第一个元素到当前节点
      leaf_page->SetKeyAt(leaf_page->GetSize(), brother_page->KeyAt(0));
      leaf_page->SetValueAt(leaf_page->GetSize(), brother_page->ValueAt(0));
      leaf_page->ChangeSizeBy(1);
      // 右兄弟元素前移
      for (int i = 0; i < brother_page->GetSize() - 1; i++) {
        brother_page->SetKeyAt(i, brother_page->KeyAt(i + 1));
        brother_page->SetValueAt(i, brother_page->ValueAt(i + 1));
      }
      brother_page->ChangeSizeBy(-1);
      // 更新父节点中的键
      int parent_index = parent->ValueIndex(brother_page_id);
      parent->SetKeyAt(parent_index, brother_page->KeyAt(0));
    }
  }

  // UNIMPLEMENTED("TODO(P2): Add implementation.");
}

// 辅助函数：处理父节点下溢
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleParentUnderflow(Context &ctx, page_id_t page_id) {
  // 节点已经没有锁了，我们需 上锁
  auto page_guard = bpm_->WritePage(page_id);
  auto page = page_guard.AsMut<InternalPage>();
  // 如果是根节点且只有一个子节点
  if (ctx.IsRootPage(page_id) && page->GetSize() <= 1) {
    // 获取唯一子节点,将其更新为根节点 ，将本节点删除
    page_id_t child_id = page->ValueAt(0);
    auto header = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
    header->root_page_id_ = child_id;
    ctx.root_page_id_ = child_id;
    // 删除旧的根节点
    page_guard = WritePageGuard{};
    bpm_->DeletePage(page_id);
    // 更新父指针
    // WritePageGuard child_guard = bpm_->WritePage(child_id);
    // BPlusTreePage *child_page = child_guard.AsMut<BPlusTreePage>();
    // child_page->SetParentPageId(INVALID_PAGE_ID);
    return;
  }

  // 如果是根节点 直接返回就行 ，因为节点数 一定大于 1
  if (ctx.IsRootPage(page_id)) {
    return;
  }
  // 如果节点的数量充裕 直接返回
  if (page->GetSize() >= page->GetMinSize()) {
    return;
  }

  // 节点的元素数量不充裕，需要重新分配或合并 兄弟节点
  int brother_flag = 0;
  page_id_t brother_id = GetBrother(ctx, page_id, brother_flag);
  if (brother_id == INVALID_PAGE_ID) {
    return;  // 没有兄弟节点 说明当前为根节点
  }

  // 获取父节点的指针
  auto parent = ctx.write_set_.back().AsMut<InternalPage>();
  // 获取兄弟节点保护 和 指针
  auto brother_guard = bpm_->WritePage(brother_id);
  auto brother = brother_guard.AsMut<InternalPage>();
  // 如果可以合并
  if (page->GetSize() + brother->GetSize() <= page->GetMaxSize()) {
    // 开始合并，我们就将右边的兄弟 合并到左边
    if (brother_flag == 1) {  // 左兄弟
      // 更新父节点  ， 在父节点中 覆盖pageid
      int parent_index = parent->ValueIndex(page_id);
      KeyType key = parent->KeyAt(parent_index);

      for (int i = parent_index; i < parent->GetSize() - 1; i++) {
        parent->SetKeyAt(i, parent->KeyAt(i + 1));
        parent->SetValueAt(i, parent->ValueAt(i + 1));
      }
      parent->ChangeSizeBy(-1);

      // 拿到对应的key之后就将key添加到brother上
      brother->SetKeyAt(brother->GetSize(), key);
      brother->SetValueAt(brother->GetSize(), page->ValueAt(0));
      // 然后将剩余的部分转移到 左兄弟节点上面
      for (int i = 1; i < page->GetSize(); i++) {
        brother->SetKeyAt(brother->GetSize() + i, page->KeyAt(i));
        brother->SetValueAt(brother->GetSize() + i, page->ValueAt(i));
      }
      brother->ChangeSizeBy(page->GetSize());
      // 删除当前节点

      page_guard = WritePageGuard{};
      bpm_->DeletePage(page_id);

      // 获取父节点 便于迭代
      page_id_t parent_id = ctx.write_set_.back().GetPageId();
      // 释放兄弟 ，还有 父节点的锁
      brother_guard = WritePageGuard{};
      ctx.write_set_.pop_back();
      HandleParentUnderflow(ctx, parent_id);
    } else {
      // 右兄弟
      // 获取右兄弟 在父节点中的index 并对其进行覆盖
      int parent_index = parent->ValueIndex(brother_id);
      KeyType key = parent->KeyAt(parent_index);

      for (int i = parent_index; i < parent->GetSize() - 1; i++) {
        parent->SetKeyAt(i, parent->KeyAt(i + 1));
        parent->SetValueAt(i, parent->ValueAt(i + 1));
      }
      parent->ChangeSizeBy(-1);
      // 获取右兄弟第一个元素对应的 key，并将其添加到当前 的节点上面

      page->SetKeyAt(page->GetSize(), key);
      page->SetValueAt(page->GetSize(), brother->ValueAt(0));

      // 再将剩下的元素进行转移
      for (int i = 1; i < brother->GetSize(); i++) {
        page->SetKeyAt(page->GetSize() + i, brother->KeyAt(i));
        page->SetValueAt(page->GetSize() + i, brother->ValueAt(i));
      }
      page->ChangeSizeBy(brother->GetSize());

      page_id_t parent_id = ctx.write_set_.back().GetPageId();
      // 释放对应的锁
      brother_guard = WritePageGuard{};
      ctx.write_set_.pop_back();
      page_guard = WritePageGuard{};
      bpm_->DeletePage(brother_id);
      HandleParentUnderflow(ctx, parent_id);
    }
  } else {
    // 如果不能合并 那我们就只能重新分配，我们尽量更新右节点的首元素
    if (brother_flag == 1) {  // 左兄弟
      // 更新父节点
      int parent_index = parent->ValueIndex(page_id);
      KeyType key = parent->KeyAt(parent_index);

      // 左兄弟要转移的pageid
      page_id_t borrow_page_id = brother->ValueAt(brother->GetSize() - 1);
      brother->ChangeSizeBy(-1);
      // 当前节点向后移动 为新节点腾出位置
      for (int i = page->GetSize(); i > 1; i--) {
        page->SetKeyAt(i, page->KeyAt(i - 1));
        page->SetValueAt(i, page->ValueAt(i - 1));
      }
      page->SetValueAt(1, page->ValueAt(0));
      page->SetValueAt(0, borrow_page_id);
      page->SetKeyAt(1, key);
      page->ChangeSizeBy(1);

      parent->SetKeyAt(parent_index, brother->KeyAt(brother->GetSize()));
    } else {  // 右边的兄弟
      // 先更新呢父节点

      int parent_index = parent->ValueIndex(brother_id);
      KeyType key = parent->KeyAt(parent_index);
      parent->SetKeyAt(parent_index, brother->KeyAt(1));

      page_id_t borrow_page_id = brother->ValueAt(0);

      brother->SetValueAt(0, brother->ValueAt(1));

      for (int i = 1; i < brother->GetSize() - 1; i++) {
        brother->SetKeyAt(i, brother->KeyAt(i + 1));
        brother->SetValueAt(i, brother->ValueAt(i + 1));
      }
      brother->ChangeSizeBy(-1);

      page->SetValueAt(page->GetSize(), borrow_page_id);
      page->SetKeyAt(page->GetSize(), key);
      page->ChangeSizeBy(1);
    }
  }
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
/*****************************************************************************
 * 索引迭代器
 *****************************************************************************/
/**
 * @brief 输入参数为void，首先找到最左侧叶子页面，然后构造
 * 索引迭代器
 *
 * 在实现任务#3时，你可能需要实现这个。
 *
 * @return : 索引迭代器
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  //  如果树为空，返回End()迭代器
  if (IsEmpty()) {
    return End();
  }
  // 创建并初始化ctx
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto *header = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;
  // 从根节点开始查找
  page_id_t page_id = ctx.root_page_id_;

  // 获取根页面
  auto guard = bpm_->WritePage(page_id);
  ctx.write_set_.push_back(std::move(guard));
  auto page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  // 循环查找最左侧的叶子节点
  while (!page->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page);

    // 始终选择最左侧的子节点 (索引0)
    page_id_t child_page_id = internal->ValueAt(0);

    // 获取子节点并继续查找
    auto child_guard = bpm_->WritePage(child_page_id);
    ctx.write_set_.push_back(std::move(child_guard));
    page = ctx.write_set_.back().AsMut<BPlusTreePage>();
  }
  // 到达叶子节点，创建迭代器
  // LeafPage  *leaf = reinterpret_cast<LeafPage *>(page);

  page_id_t leaf_id = ctx.write_set_.back().GetPageId();
  // 清理上下文中的读取保护
  ctx.write_set_.clear();

  // 返回迭代器，指向该叶子节点的第一个元素
  return IndexIterator<KeyType, ValueType, KeyComparator>(bpm_, leaf_id, 0);
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
/**
 * @brief 输入参数是低键，首先找到包含输入键的叶子页面
 * 然后构造索引迭代器
 * @return : 索引迭代器
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  Context ctx;
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto header = ctx.header_page_.value().AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header->root_page_id_;

  LeafPage *leaf_page = FindLeafPage(ctx, key, Operation::INSERT);
  page_id_t leaf_id = ctx.write_set_.back().GetPageId();

  int index = -1;
  while (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) != 0) {
    index++;
  }
  if (index == -1) {
    std::cout << "wrong index" << std::endl;
    // throw Exception("wrong index");
  }

  ctx.write_set_.clear();

  return IndexIterator<KeyType, ValueType, KeyComparator>(bpm_, leaf_id, index);
}

/**
 * @brief Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
/**
 * @brief 输入参数为void，构造一个索引迭代器，表示
 * 叶子节点中键/值对的末尾
 * @return : 索引迭代器
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  // 就是返回一个无效的迭代器
  return IndexIterator<KeyType, ValueType, KeyComparator>::End();
}

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
/**
 * @return 此树根部的页面ID
 *
 * 在实现任务#3时，你可能需要实现这个。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);  // 获取头页面的读取保护
  auto root_page = guard.As<BPlusTreeHeaderPage>();       // 获取头页面的指针

  return root_page->root_page_id_;
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub