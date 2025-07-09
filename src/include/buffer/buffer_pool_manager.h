//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

/**
 * @brief A helper class for `BufferPoolManager` that manages a frame of memory and related metadata.
 *
 * This class represents headers for frames of memory that the `BufferPoolManager` stores pages of data into. Note that
 * the actual frames of memory are not stored directly inside a `FrameHeader`, rather the `FrameHeader`s store pointer
 * to the frames and are stored separately them.
 *
 * ---
 *
 * Something that may (or may not) be of interest to you is why the field `data_` is stored as a vector that is
 * allocated on the fly instead of as a direct pointer to some pre-allocated chunk of memory.
 *
 * In a traditional production buffer pool manager, all memory that the buffer pool is intended to manage is allocated
 * in one large contiguous array (think of a very large `malloc` call that allocates several gigabytes of memory up
 * front). This large contiguous block of memory is then divided into contiguous frames. In other words, frames are
 * defined by an offset from the base of the array in page-sized (4 KB) intervals.
 *
 * In BusTub, we instead allocate each frame on its own (via a `std::vector<char>`) in order to easily detect buffer
 * overflow with address sanitizer. Since C++ has no notion of memory safety, it would be very easy to cast a page's
 * data pointer into some large data type and start overwriting other pages of data if they were all contiguous.
 *
 * If you would like to attempt to use more efficient data structures for your buffer pool manager, you are free to do
 * so. However, you will likely benefit significantly from detecting buffer overflow in future projects (especially
 * project 2).
 */
/**
 * @brief `BufferPoolManager`的辅助类，用于管理内存帧及其相关元数据。
 *
 * 这个类表示`BufferPoolManager`将数据页存储到的内存帧的头信息。注意，
 * 实际的内存帧并不直接存储在`FrameHeader`内部，而是`FrameHeader`存储指向
 * 这些帧的指针，帧与它们是分开存储的。
 *
 * ---
 *
 * 可能会（或可能不会）引起你兴趣的是为什么`data_`字段被存储为一个动态分配的向量
 * 而不是指向预分配内存块的直接指针。
 *
 * 在传统的生产级缓冲池管理器中，缓冲池管理的所有内存被分配在一个大的连续数组中
 * （想象一个非常大的`malloc`调用，预先分配几个千兆字节的内存）。这个大的连续内存块
 * 然后被分割成连续的帧。换句话说，帧是由从数组基地址开始按页大小（4 KB）间隔偏移
 * 定义的。
 *
 * 在BusTub中，我们通过`std::vector<char>`单独分配每个帧，以便使用地址消毒器
 * 轻松检测缓冲区溢出。由于C++没有内存安全的概念，如果所有内存都是连续的，很容易将
 * 页面的数据指针转换为某个大型数据类型，并开始覆盖其他页面的数据。
 *
 * 如果你想尝试为缓冲池管理器使用更高效的数据结构，你可以自由实现。不过，在未来的项目中
 * （特别是项目2），你很可能会从检测缓冲区溢出中获益显著。
 */
class FrameHeader {
  friend class BufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;

 public:
  explicit FrameHeader(frame_id_t frame_id);

 private:
  auto GetData() const -> const char *;
  auto GetDataMut() -> char *;
  void Reset();

  /** @brief The frame ID / index of the frame this header represents. */
  /** @brief 此头部表示的帧的帧ID/索引。 */
  const frame_id_t frame_id_;

  /** @brief The readers / writer latch for this frame. */
  /** @brief 此帧的读者/写者闩锁。 */
  std::shared_mutex rwlatch_;

  /** @brief The number of pins on this frame keeping the page in memory. */
  /** @brief 此帧上保持页面在内存中的引用数量。 */
  std::atomic<size_t> pin_count_;

  /** @brief The dirty flag. */
  /** @brief 脏标记。 */
  bool is_dirty_;

  /**
   * @brief A pointer to the data of the page that this frame holds.
   *
   * If the frame does not hold any page data, the frame contains all null bytes.
   */
  /**
   * @brief 指向此帧持有的页面数据的指针。
   *
   * 如果帧不持有任何页面数据，帧包含全部为空字节。
   */
  std::vector<char> data_;

  /**
   * TODO(P1): You may add any fields or helper functions under here that you think are necessary.
   *
   * One potential optimization you could make is storing an optional page ID of the page that the `FrameHeader` is
   * currently storing. This might allow you to skip searching for the corresponding (page ID, frame ID) pair somewhere
   * else in the buffer pool manager...
   */
  /**
   * TODO(P1): 你可以在此处添加任何你认为必要的字段或辅助函数。
   *
   * 一个潜在的优化是存储`FrameHeader`当前存储的页面ID的可选项。
   * 这可能允许你跳过在缓冲池管理器的其他地方搜索相应的(页面ID, 帧ID)对...
   */
};

/**
 * @brief The declaration of the `BufferPoolManager` class.
 *
 * As stated in the writeup, the buffer pool is responsible for moving physical pages of data back and forth from
 * buffers in main memory to persistent storage. It also behaves as a cache, keeping frequently used pages in memory for
 * faster access, and evicting unused or cold pages back out to storage.
 *
 * Make sure you read the writeup in its entirety before attempting to implement the buffer pool manager. You also need
 * to have completed the implementation of both the `LRUKReplacer` and `DiskManager` classes.
 */
/**
 * @brief `BufferPoolManager`类的声明。
 *
 * 如项目说明所述，缓冲池负责在主内存中的缓冲区和持久存储之间来回移动数据的物理页面。
 * 它也表现为一个缓存，将频繁使用的页面保留在内存中以便更快地访问，并将未使用或冷页面驱逐回存储。
 *
 * 确保在尝试实现缓冲池管理器之前完整阅读项目说明。你还需要完成`LRUKReplacer`和`DiskManager`类的实现。
 */
class BufferPoolManager {
 public:
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist = LRUK_REPLACER_K,
                    LogManager *log_manager = nullptr);
  ~BufferPoolManager();

  auto Size() const -> size_t;
  auto NewPage() -> page_id_t;
  auto DeletePage(page_id_t page_id) -> bool;

  auto CheckedWritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<WritePageGuard>;
  auto CheckedReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> std::optional<ReadPageGuard>;
  auto WritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> WritePageGuard;
  auto ReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> ReadPageGuard;
  auto FlushPageUnsafe(page_id_t page_id) -> bool;
  auto FlushPage(page_id_t page_id) -> bool;
  void FlushAllPagesUnsafe();
  void FlushAllPages();
  auto GetPinCount(page_id_t page_id) -> std::optional<size_t>;

 private:
  /** @brief The number of frames in the buffer pool. */
  /** @brief 缓冲池中的帧数量。 */
  const size_t num_frames_;

  /** @brief The next page ID to be allocated.  */
  /** @brief 下一个要分配的页面ID。  */
  std::atomic<page_id_t> next_page_id_;

  /**
   * @brief The latch protecting the buffer pool's inner data structures.
   *
   * TODO(P1) We recommend replacing this comment with details about what this latch actually protects.
   */
  /**
   * @brief 保护缓冲池内部数据结构的闩锁。
   *
   * TODO(P1) 我们建议将此注释替换为关于此闩锁实际保护内容的详细信息。
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /** @brief The frame headers of the frames that this buffer pool manages. */
  /** @brief 此缓冲池管理的帧的帧头。 */
  std::vector<std::shared_ptr<FrameHeader>> frames_;

  /** @brief The page table that keeps track of the mapping between pages and buffer pool frames. */
  /** @brief 页表，用于跟踪页面和缓冲池帧之间的映射关系。 */
  std::unordered_map<page_id_t, frame_id_t> page_table_;

  /** @brief A list of free frames that do not hold any page's data. */
  /** @brief 不持有任何页面数据的空闲帧列表。 */
  std::list<frame_id_t> free_frames_;

  /** @brief The replacer to find unpinned / candidate pages for eviction. */
  /** @brief 用于查找未固定/可驱逐的页面的替换器。 */
  std::shared_ptr<LRUKReplacer> replacer_;

  /** @brief A pointer to the disk scheduler. Shared with the page guards for flushing. */
  /** @brief 指向磁盘调度器的指针。与页面守卫共享用于刷新操作。 */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief A pointer to the log manager.
   *
   * Note: Please ignore this for P1.
   */
  /**
   * @brief 指向日志管理器的指针。
   *
   * 注意：请在P1中忽略此内容。
   */
  LogManager *log_manager_ __attribute__((__unused__));

  /**
   * TODO(P1): You may add additional private members and helper functions if you find them necessary.
   *
   * There will likely be a lot of code duplication between the different modes of accessing a page.
   *
   * We would recommend implementing a helper function that returns the ID of a frame that is free and has nothing
   * stored inside of it. Additionally, you may also want to implement a helper function that returns either a shared
   * pointer to a `FrameHeader` that already has a page's data stored inside of it, or an index to said `FrameHeader`.
   */
  /**
   * TODO(P1): 如果你觉得必要，可以添加额外的私有成员和辅助函数。
   *
   * 不同的页面访问模式之间可能会有很多代码重复。
   *
   * 我们建议实现一个辅助函数，返回一个空闲且内部没有存储任何内容的帧的ID。
   * 此外，你可能还想实现一个辅助函数，返回一个已经存储了页面数据的`FrameHeader`的共享指针，
   * 或者该`FrameHeader`的索引。
   */
};
}  // namespace bustub
