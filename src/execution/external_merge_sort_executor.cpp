//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <iostream>
#include <optional>
#include <vector>
#include "common/config.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/page_guard.h"
#include <algorithm>
#include "execution/execution_common.h"

namespace bustub {

// SortPage Implementation
void SortPage::Init(const Schema *schema) {
  auto header = GetHeader();
  header->tuple_count_ = 0;
  header->tuple_size_ = schema->GetInlinedStorageSize();
  
  size_t available_space = BUSTUB_PAGE_SIZE - sizeof(SortPageHeader);
  header->max_tuple_count_ = available_space / header->tuple_size_;
}

auto SortPage::InsertTuple(const Tuple &tuple) -> bool {
  if (IsFull()) {
    return false;
  }
  
  auto header = GetHeader();
  char *tuple_data = GetTupleData() + header->tuple_count_ * header->tuple_size_;
  
  // Copy tuple data directly (fixed-length only)
  memcpy(tuple_data, tuple.GetData(), header->tuple_size_);
  header->tuple_count_++;
  
  return true;
}

auto SortPage::GetTuple(size_t index) const -> Tuple {
  auto header = GetHeader();
  if (index >= header->tuple_count_) {
    throw std::out_of_range("Tuple index out of range");
  }
  
  const char *tuple_data = GetTupleData() + index * header->tuple_size_;
  return Tuple(RID{}, tuple_data, header->tuple_size_);
}

auto SortPage::GetTupleCount() const -> size_t {
  return GetHeader()->tuple_count_;
}

auto SortPage::GetMaxTupleCount() const -> size_t {
  return GetHeader()->max_tuple_count_;
}

auto SortPage::IsFull() const -> bool {
  auto header = GetHeader();
  return header->tuple_count_ >= header->max_tuple_count_;
}

void SortPage::Clear() {
  GetHeader()->tuple_count_ = 0;
}

// MergeSortRun::Iterator Implementation
auto MergeSortRun::Iterator::operator++() -> Iterator & {
  tuple_index_++;
  
  // Check if we need to move to next page
  if (current_page_ && tuple_index_ >= current_page_->GetTupleCount()) {
    page_index_++;
    tuple_index_ = 0;
    
    if (page_index_ < run_->pages_.size()) {
      // Load next page
      auto page_guard = run_->bpm_->ReadPage(run_->pages_[page_index_]);
      current_page_ = reinterpret_cast<const SortPage *>(page_guard.GetData());
      page_guard_ = std::move(page_guard);
    } else {
      // End of run
      current_page_ = nullptr;
      page_guard_ = ReadPageGuard{};
    }
  }
  
  return *this;
}

auto MergeSortRun::Iterator::operator*() -> Tuple {
  if (!current_page_ || tuple_index_ >= current_page_->GetTupleCount()) {
    throw std::runtime_error("Iterator out of range");
  }
  return current_page_->GetTuple(tuple_index_);
}

auto MergeSortRun::Iterator::operator==(const Iterator &other) const -> bool {
  return run_ == other.run_ && page_index_ == other.page_index_ && tuple_index_ == other.tuple_index_;
}

auto MergeSortRun::Iterator::operator!=(const Iterator &other) const -> bool {
  return !(*this == other);
}

auto MergeSortRun::Begin() const -> Iterator {
  Iterator it(this);
  if (!pages_.empty()) {
    it.page_index_ = 0;
    it.tuple_index_ = 0;
    auto page_guard = bpm_->ReadPage(pages_[0]);
    it.current_page_ = reinterpret_cast<const SortPage *>(page_guard.GetData());
    it.page_guard_ = std::move(page_guard);
    
    // Skip to first valid tuple if current position is invalid
    if (it.current_page_ && it.current_page_->GetTupleCount() == 0) {
      ++it;
    }
  }
  return it;
}

auto MergeSortRun::End() const -> Iterator {
  Iterator it(this);
  it.page_index_ = pages_.size();
  it.tuple_index_ = 0;
  it.current_page_ = nullptr;
  return it;
}


template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();
  
  // Phase 1: Create initial sorted runs
  std::vector<MergeSortRun> runs = CreateInitialRuns();
  
  // Phase 2: Merge runs until only one remains
  while (runs.size() > 1) {
    runs = MergeRuns(runs);
  }
  
  // Set up iterator for final result
  if (!runs.empty()) {
    final_run_ = std::move(runs[0]);
    result_iterator_ = final_run_.Begin();
  }
}

/**
 * Yield the next tuple from the external merge sort.
 * @param[out] tuple The next tuple produced by the external merge sort.
 * @param[out] rid The next tuple RID produced by the external merge sort.
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_iterator_ == final_run_.End()) {
    return false;
  }
  
  *tuple = *result_iterator_;
  *rid = RID{}; // External sort doesn't preserve original RIDs
  ++result_iterator_;
  
  return true;
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::CreateInitialRuns() -> std::vector<MergeSortRun> {
  std::vector<MergeSortRun> runs;
  
  current_page_id_ = exec_ctx_->GetBufferPoolManager()->NewPage();
  auto new_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id_);
  auto sort_page = reinterpret_cast<SortPage *>(new_page_guard.GetDataMut());
  sort_page->Init(&plan_->OutputSchema());
  
  Tuple tuple;
  RID rid;
  
  while (child_executor_->Next(&tuple, &rid)) {
    if (sort_page->IsFull()) {
      // Sort current page and create run
      SortPageTuples(sort_page);
      
      std::vector<page_id_t> run_pages = {current_page_id_};
      runs.emplace_back(std::move(run_pages), exec_ctx_->GetBufferPoolManager(), &plan_->OutputSchema());
      
      // Get new page
      current_page_id_ = exec_ctx_->GetBufferPoolManager()->NewPage();
      new_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_page_id_);
      sort_page = reinterpret_cast<SortPage *>(new_page_guard.GetDataMut());
      sort_page->Init(&plan_->OutputSchema());
    }
    
    sort_page->InsertTuple(tuple);
  }
  
  // Handle last page if it has tuples
  if (sort_page->GetTupleCount() > 0) {
    SortPageTuples(sort_page);
    std::vector<page_id_t> run_pages = {current_page_id_};
    runs.emplace_back(std::move(run_pages), exec_ctx_->GetBufferPoolManager(), &plan_->OutputSchema());
  } else {
    // Delete empty page
    exec_ctx_->GetBufferPoolManager()->DeletePage(current_page_id_);
  }
  
  return runs;
}

template <size_t K>
void ExternalMergeSortExecutor<K>::SortPageTuples(SortPage *page) {
  size_t tuple_count = page->GetTupleCount();
  if (tuple_count <= 1) {
    return;
  }
  
  // Extract tuples into vector and generate SortEntry for each
  std::vector<SortEntry> entries;
  entries.reserve(tuple_count);
  
  for (size_t i = 0; i < tuple_count; i++) {
    Tuple tuple = page->GetTuple(i);
    auto sort_key = GenerateSortKey(tuple, plan_->GetOrderBy(), plan_->OutputSchema());
    entries.emplace_back(std::move(sort_key), std::move(tuple));
  }
  
  // Sort using TupleComparator
  std::sort(entries.begin(), entries.end(), cmp_);
  
  // Write back sorted tuples
  page->Clear();
  for (const auto &entry : entries) {
    page->InsertTuple(entry.second);
  }
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::MergeRuns(const std::vector<MergeSortRun> &input_runs) -> std::vector<MergeSortRun> {
  std::vector<MergeSortRun> output_runs;
  
  // Merge pairs of runs
  for (size_t i = 0; i < input_runs.size(); i += 2) {
    if (i + 1 < input_runs.size()) {
      // Merge two runs
      auto merged_run = MergeTwoRuns(input_runs[i], input_runs[i + 1]);
      output_runs.push_back(std::move(merged_run));
    } else {
      // Odd run, just copy it
      output_runs.push_back(input_runs[i]);
    }
  }
  
  // Delete pages from input runs
  for (const auto &run : input_runs) {
    for (size_t i = 0; i < run.GetPageCount(); i++) {
      exec_ctx_->GetBufferPoolManager()->DeletePage(run.GetPageId(i));
    }
  }
  
  return output_runs;
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::MergeTwoRuns(const MergeSortRun &run1, const MergeSortRun &run2) -> MergeSortRun {
  std::vector<page_id_t> output_pages;
  
  auto iter1 = run1.Begin();
  auto iter2 = run2.Begin();
  
  // Get first output page
  page_id_t current_output_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
  auto output_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_output_page_id);
  auto output_page = reinterpret_cast<SortPage *>(output_page_guard.GetDataMut());
  output_page->Init(&plan_->OutputSchema());
  
  while (iter1 != run1.End() && iter2 != run2.End()) {
    Tuple tuple_to_insert;
    
    // Choose smaller tuple by comparing sort keys
    Tuple tuple1 = *iter1;
    Tuple tuple2 = *iter2;
    auto key1 = GenerateSortKey(tuple1, plan_->GetOrderBy(), plan_->OutputSchema());
    auto key2 = GenerateSortKey(tuple2, plan_->GetOrderBy(), plan_->OutputSchema());
    SortEntry entry1(std::move(key1), tuple1);
    SortEntry entry2(std::move(key2), tuple2);
    
    if (cmp_(entry1, entry2)) {
      tuple_to_insert = tuple1;
      ++iter1;
    } else {
      tuple_to_insert = tuple2;
      ++iter2;
    }
    
    // Insert tuple, get new page if current is full
    if (output_page->IsFull()) {
      output_pages.push_back(current_output_page_id);
      current_output_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      output_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_output_page_id);
      output_page = reinterpret_cast<SortPage *>(output_page_guard.GetDataMut());
      output_page->Init(&plan_->OutputSchema());
    }
    
    output_page->InsertTuple(tuple_to_insert);
  }
  
  // Handle remaining tuples from run1
  while (iter1 != run1.End()) {
    if (output_page->IsFull()) {
      output_pages.push_back(current_output_page_id);
      current_output_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      output_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_output_page_id);
      output_page = reinterpret_cast<SortPage *>(output_page_guard.GetDataMut());
      output_page->Init(&plan_->OutputSchema());
    }
    
    output_page->InsertTuple(*iter1);
    ++iter1;
  }
  
  // Handle remaining tuples from run2
  while (iter2 != run2.End()) {
    if (output_page->IsFull()) {
      output_pages.push_back(current_output_page_id);
      current_output_page_id = exec_ctx_->GetBufferPoolManager()->NewPage();
      output_page_guard = exec_ctx_->GetBufferPoolManager()->WritePage(current_output_page_id);
      output_page = reinterpret_cast<SortPage *>(output_page_guard.GetDataMut());
      output_page->Init(&plan_->OutputSchema());
    }
    
    output_page->InsertTuple(*iter2);
    ++iter2;
  }
  
  // Add final page if it has tuples
  if (output_page->GetTupleCount() > 0) {
    output_pages.push_back(current_output_page_id);
  } else {
    exec_ctx_->GetBufferPoolManager()->DeletePage(current_output_page_id);
  }
  
  return MergeSortRun(std::move(output_pages), exec_ctx_->GetBufferPoolManager(), &plan_->OutputSchema());
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
