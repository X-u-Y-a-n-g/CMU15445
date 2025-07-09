//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Fall 2024.
 */
class SortPage {
 public:
  /**
   * TODO: Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */

   /**
   * Initialize the sort page with schema information.
   */
  void Init(const Schema *schema);

  /**
   * Insert a tuple into the sort page.
   * @param tuple The tuple to insert
   * @return true if successful, false if page is full
   */
  auto InsertTuple(const Tuple &tuple) -> bool;

  /**
   * Get a tuple at the specified index.
   * @param index The index of the tuple
   * @return The tuple at the index
   */
  auto GetTuple(size_t index) const -> Tuple;

  /**
   * Get the number of tuples in the page.
   * @return The number of tuples
   */
  auto GetTupleCount() const -> size_t;

  /**
   * Get the maximum number of tuples that can fit in the page.
   * @return The maximum tuple count
   */
  auto GetMaxTupleCount() const -> size_t;

  /**
   * Check if the page is full.
   * @return true if full, false otherwise
   */
  auto IsFull() const -> bool;

  /**
   * Clear all tuples from the page.
   */
  void Clear();


 private:
  /**
   * TODO: Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */

   /** Header information */
  struct SortPageHeader {
    size_t tuple_count_;
    size_t tuple_size_;
    size_t max_tuple_count_;
  };

  /** Get the header of the page */
  auto GetHeader() -> SortPageHeader * { return reinterpret_cast<SortPageHeader *>(data_); }
  auto GetHeader() const -> const SortPageHeader * { return reinterpret_cast<const SortPageHeader *>(data_); }

  /** Get the start of tuple data */
  auto GetTupleData() -> char * { return data_ + sizeof(SortPageHeader); }
  auto GetTupleData() const -> const char * { return data_ + sizeof(SortPageHeader); }

  /** Page data */
  char data_[BUSTUB_PAGE_SIZE];

};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm, const Schema *schema) 
      : pages_(std::move(pages)), bpm_(bpm), schema_(schema) {}

  auto GetPageCount() const -> size_t { return pages_.size(); }
  auto GetPageId(size_t index) const -> page_id_t { return pages_[index]; }



  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     *
     * TODO: Implement this method.
     */
    auto operator++() -> Iterator &; 

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     *
     * TODO: Implement this method.
     */
    auto operator*() -> Tuple;

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     *
     * TODO: Implement this method.
     */
    auto operator==(const Iterator &other) const -> bool;

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     *
     * TODO: Implement this method.
     */
    auto operator!=(const Iterator &other) const -> bool;

   private:
    explicit Iterator(const MergeSortRun *run) : run_(run), page_index_(0), tuple_index_(0), current_page_(nullptr) {}

    /** The sorted run that the iterator is iterating on. */
    [[maybe_unused]] const MergeSortRun *run_;

    /**
     * TODO: Add your own private members here. You may want something to record your current
     * position in the sorted run. Also feel free to add additional constructors to initialize
     * your private members.
     */
    size_t page_index_{0};
    size_t tuple_index_{0};
    const SortPage *current_page_{nullptr};
    ReadPageGuard page_guard_;


  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   *
   * TODO: Implement this method.
   */
  auto Begin() const -> Iterator;

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   *
   * TODO: Implement this method.
   */
  auto End() const -> Iterator;

 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  [[maybe_unused]] BufferPoolManager *bpm_;

  /** Schema for the tuples in this run */
  const Schema *schema_{nullptr};
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Fall 2024, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  /** TODO: You will want to add your own private members here. */

  /** Child executor */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** Current page being processed */
  page_id_t current_page_id_;

  /** Final sorted run */
  MergeSortRun final_run_;

  /** Iterator for result output */
  MergeSortRun::Iterator result_iterator_;

  /** Helper methods */
  auto CreateInitialRuns() -> std::vector<MergeSortRun>;
  void SortPageTuples(SortPage *page);
  auto MergeRuns(const std::vector<MergeSortRun> &input_runs) -> std::vector<MergeSortRun>;
  auto MergeTwoRuns(const MergeSortRun &run1, const MergeSortRun &run2) -> MergeSortRun;


};

}  // namespace bustub
