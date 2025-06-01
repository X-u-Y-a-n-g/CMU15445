//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_page.cpp
//
// Identification: src/storage/page/b_plus_tree_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { 
    //UNIMPLEMENTED("TODO(P2): Add implementation."); 
    return this->page_type_ == IndexPageType::LEAF_PAGE;    
}
void BPlusTreePage::SetPageType(IndexPageType page_type) { 
    //UNIMPLEMENTED("TODO(P2): Add implementation."); 
    this->page_type_ = page_type;
}
auto BPlusTreePage::GetPageType() const -> IndexPageType { 
    //UNIMPLEMENTED("TODO(P2): Add implementation."); 
    return page_type_;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { 
    //UNIMPLEMENTED("TODO(P2): Add implementation."); 
    return this->size_;
}
void BPlusTreePage::SetSize(int size) { 
    //UNIMPLEMENTED("TODO(P2): Add implementation."); 
    if (size > this->max_size_) {
        abort();
    }
    this->size_ = size;
}
void BPlusTreePage::ChangeSizeBy(int amount) { 
    //UNIMPLEMENTED("TODO(P2): Add implementation."); 
    if (this->size_ + amount > GetMaxSize()) {
        abort();
    }
    this->size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { 
    //UNIMPLEMENTED("TODO(P2): Add implementation."); 
    return this->max_size_;
}
void BPlusTreePage::SetMaxSize(int size) { 
    //UNIMPLEMENTED("TODO(P2): Add implementation."); 
    if (size <= 0) {
        throw Exception("Max size must be greater than 0");
    }
    if (size > INT_MAX) {
        throw Exception("Max size must be less than INT_MAX");
    }
    this->max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * But whether you will take ceil() or floor() depends on your implementation
 */
auto BPlusTreePage::GetMinSize() const -> int { 
    //UNIMPLEMENTED("TODO(P2): Add implementation."); 
    return (this->max_size_ + 1) / 2;
}

}  // namespace bustub