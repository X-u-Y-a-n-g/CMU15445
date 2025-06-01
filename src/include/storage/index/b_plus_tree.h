//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.h
//
// Identification: src/include/storage/index/b_plus_tree.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <filesystem>
#include <iostream>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  enum class Operation {
    READ,    // 读操作
    INSERT,  // 插入操作
    DELETE,  // 删除操作
  };

  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SLOT_CNT,
                     int internal_max_size = INTERNAL_PAGE_SLOT_CNT);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value) -> bool;
  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key);

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // 得到节点 pageNode  的 兄弟节点 的page _id  flag = 1(左) 2（右）
  auto GetBrother(Context& ctx,page_id_t cur_node_id,int & flag) -> page_id_t ; 

  // 叶子节点的 将旧节点向新节点分裂 分裂函数 返回 被分裂的节点的首元素,
  auto leafSplit (page_id_t &old_node,page_id_t & new_node) -> KeyType;

  // // 叶子节点的删除函数 ，将叶子节点中的 某一个键为 key 的节点（页面） 进行删除
  // auto leafDelete(page_id_t & node , KeyType & key) ->bool;
  
  // // 将一个key - newpage 插入节点 node 
  // auto leafInsert(page_id_t &node , KeyType key, ValueType newpage) ->bool;

  // // 叶子节点的合并 将newnode 节点转移到old node上面,其余属性均没有修改
  // auto leafMerge(page_id_t &old_node, page_id_t &new_node) ->bool;

  // //内部节点的合并 ，将newnode合并到 old node上面没有修改属性
  // auto interMerge( page_id_t &old_node, page_id_t &new_node) ->bool;

    
  // //在内部节点node上面 插入一个新的节点
  // auto interInsert(page_id_t &node , KeyType key , ValueType value) ->bool;

  // //在内部节点中删除 key 为key 所对应的元素
  // auto interDelete( page_id_t &node , KeyType key) ->bool;


  //找到应该插入的节点所对应的叶子节点；
  auto FindLeafPage(Context &ctx, const KeyType &key, Operation op) -> LeafPage*;

  //迭代插入 
  auto InsertIntoParent(Context &ctx, page_id_t  left_page_id, const KeyType &middle_key, page_id_t right_page_id) ->bool;

  //处理删除过程中的父节点
  void HandleParentUnderflow(Context &ctx, page_id_t page_id);


  // // 找到最小的key page_id中的所有叶子节点的最小值 
  // auto FindMinimumKey(page_id_t page_id) -> KeyType ;
  
  void UpdateParentKeyAfterDeletion(Context &ctx, page_id_t parent_id, page_id_t deleted_child_id);

  
  // Index iterator
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  void Print(BufferPoolManager *bpm);

  void Draw(BufferPoolManager *bpm, const std::filesystem::path &outf);

  auto DrawBPlusTree() -> std::string;

  // read data from file and insert one by one
  void InsertFromFile(const std::filesystem::path &file_name);

  // read data from file and remove one by one
  void RemoveFromFile(const std::filesystem::path &file_name);

  void BatchOpsFromFile(const std::filesystem::path &file_name);

 private:
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;


  // // 用于控制访问的锁
  // mutable std::shared_mutex tree_mutex_;
  // mutable std::shared_mutex insert_mutex;
  // mutable std::shared_mutex delete_mutex;
};

/**
 * @brief for test only. PrintableBPlusTree is a printable B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub