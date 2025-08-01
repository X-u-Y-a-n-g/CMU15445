//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
/**
 * @brief `FrameHeader` 的构造函数，将所有字段初始化为默认值。
 *
 * 有关更多信息，请参见 "buffer/buffer_pool_manager.h" 中 `FrameHeader` 的文档。
 *
 * @param frame_id 我们正在为其创建头的帧的ID/索引。
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
/**
 * @brief 获取指向帧数据的原始常量指针。只能读取，不能修改。
 *
 * @return const char* 指向帧存储的不可变数据的指针。
 *
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
/**
 * @brief 获取指向帧数据的原始可变指针。可以读取和修改。
 *
 * @return char* 指向帧存储的可变数据的指针。
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
/**
 * @brief 重置 `FrameHeader` 的成员字段。
 */

void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */

/**
 * @brief 创建一个新的 `BufferPoolManager` 实例并初始化所有字段。
 *
 * 有关更多信息，请参见 "buffer/buffer_pool_manager.h" 中 `BufferPoolManager` 的文档。
 *
 * ### 实现
 *
 * 我们以一种与我们的参考解决方案相符的方式为您实现了构造函数。如果它不适合您的实现，您可以自由地更改您想要的任何内容。
 *
 * 但要警告一下！如果您偏离我们的指导太远，我们将更难帮助您。我们的建议是首先使用我们提供的基石来实现缓冲池管理器。
 *
 * 一旦您有了一个完全可工作的解决方案（所有Gradescope测试用例都通过了），那么您可以尝试更有趣的事情！
 *
 * @param num_frames 缓冲池的大小。
 * @param disk_manager 磁盘管理器。
 * @param k_dist LRU-K替换器的向后k距离。
 * @param log_manager 日志管理器。请忽略P1的这个参数。
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // 不是严格必要的...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);  //  原子储存操作

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);  //  却包邮足够的内存

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);  // 确保内存

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 *
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.返回此缓冲池管理的帧数。
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
/**
 * @brief 在磁盘上分配一个新页面。
 *
 * ### 实现
 *
 * 您将维护一个线程安全的、单调递增的计数器，形式为 `std::atomic<page_id_t>`。
 * 有关更多信息，请参见[原子](https://en.cppreference.com/w/cpp/atomic/atomic)的文档。
 *
 * TODO(P1): 添加实现。
 *
 * @return 新分配页面的页面ID。
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  //  加锁，确保线程安全
  std::unique_lock lock(*bpm_latch_);

  // 通过原子计数器获取一个新的页面ID，确保页面ID单调递增
  page_id_t new_page_id = next_page_id_.fetch_add(1);  //  原子操作，获取当前值并自增1
  frame_id_t frame_id = INVALID_FRAME_ID;
  std::shared_ptr<FrameHeader> frame_header = nullptr;

  // 情况1：如果存在空闲帧，则直接从空闲帧列表中取出一个帧
  if (!free_frames_.empty()) {
    frame_id = free_frames_.front();   // 获取空闲帧ID
    free_frames_.pop_front();          // 移除空闲帧
    frame_header = frames_[frame_id];  //
  } else {
    // 情况2：没有空闲帧，则通过替换器查找可驱逐的帧
    auto evicted_frame_id = replacer_->Evict();  // 可驱逐帧的id
    if (!evicted_frame_id.has_value()) {
      // 如果替换器没有返回可驱逐的帧，说明所有帧都在使用，返回无效页面ID
      return INVALID_PAGE_ID;
    }

    frame_id = evicted_frame_id.value();
    frame_header = frames_[frame_id];  // FrameHeader

    // 找到此帧当前存储的旧页面ID
    page_id_t old_page_id = INVALID_PAGE_ID;

    for (auto &page_frame : page_table_) {
      if (page_frame.second == frame_id) {
        old_page_id = page_frame.first;
        break;
      }
    }
    // 刷新旧页面
    FlushPageUnsafe(old_page_id);
    // 从页表中移除旧页面的映射
    page_table_.erase(old_page_id);
  }

  // 重置帧，清空原有数据，同时将 pin_count 重置为0、is_dirty_ 置为 false
  frame_header->Reset();
  // 固定页面（pin_count设为1），表明该帧上的页面正在被引用次数为1
  frame_header->pin_count_.store(1);

  // 将新页面ID与选定的帧ID建立映射关系，更新页表
  page_table_[new_page_id] = frame_id;

  // 通知替换器，此帧刚刚被访问，并设置其为不可驱逐状态（因为当前被固定）
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // 返回新分配的页面ID
  return new_page_id;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places a page or a page's metadata could be, and use that to guide you on implementing this
 * function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * You should call `DeallocatePage` in the disk scheduler to make the space available for new pages.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
/**
 * @brief 从数据库中移除一个页面，包括磁盘和内存中的页面。
 *
 * 如果页面在缓冲池中被固定，此函数不执行任何操作并返回 `false`。否则，此函数
 * 从磁盘和内存中移除页面（如果它仍在缓冲池中），返回 `true`。
 *
 * ### 实现
 *
 * 思考一个页面或页面的元数据可能存在的所有地方，并用它来指导您实现这个函数。
 * 您可能想在实现了 `CheckedReadPage` 和 `CheckedWritePage` 之后实现这个函数。
 *
 * 理想情况下，我们希望确保磁盘上的所有空间都被高效使用。这意味着被删除页面在磁盘上
 * 曾经占用的空间应该以某种方式提供给由 `NewPage` 分配的新页面使用。
 *
 * 如果您想尝试这样做，您可以自由尝试。但是，对于这个实现，您可以假设您不会用完磁盘空间，
 * 并在 `NewPage` 中简单地持续向上分配磁盘空间。
 *
 * 为了（不存在的）风格分数，您仍然可以调用 `DeallocatePage`，以防您将来想实现稍微
 * 更节省空间的东西。
 *
 * TODO(P1): 添加实现。
 *
 * @param page_id 我们想要删除的页面的页面ID。
 * @return 如果页面存在但无法删除，则为 `false`；如果页面不存在或删除成功，则为 `true`。
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  //  对缓冲池内部数据结构上锁，保证线程安全
  std::scoped_lock lock(*bpm_latch_);

  // 如果页表中不包含该页面，说明该页面已经不在缓冲池中，
  // 我们可以认为删除操作成功
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  // 获取该页面对应的帧号以及对应的帧头
  frame_id_t frame_id = page_table_[page_id];
  std::shared_ptr<FrameHeader> frame_header = frames_[frame_id];

  // 如果页面的 pin_count 大于 0，说明页面正在被使用，此时不能删除
  if (frame_header->pin_count_.load() > 0) {
    return false;
  }
  if (!FlushPageUnsafe(page_id)) {
    return false;
  }
  // 从页表中移除该页面的映射
  page_table_.erase(page_id);

  // 重置帧，将其中的数据清空、pin_count 重置为 0 等（注意 Reset() 会将内存数据清零）
  frame_header->Reset();

  // 此时该帧已经不存储任何页面数据，将其放回空闲帧列表，
  free_frames_.push_back(frame_id);

  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are 3 main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
/**
 * @brief 获取对数据页面的可选写锁守卫。用户可以根据需要指定 `AccessType`。
 *
 * 如果无法将数据页面加载到内存中，此函数将返回 `std::nullopt`。
 *
 * 页面数据只能通过页面守卫访问。此 `BufferPoolManager` 的用户应该根据他们希望访问数据的
 * 模式获取 `ReadPageGuard` 或 `WritePageGuard`，这确保了任何数据访问都是线程安全的。
 *
 * 一次只能有1个 `WritePageGuard` 读取/写入一个页面。这允许数据访问既可以是不可变的也可以是
 * 可变的，这意味着拥有 `WritePageGuard` 的线程可以按照他们想要的方式操作页面的数据。
 * 如果用户想要多个线程同时读取页面，他们必须使用 `CheckedReadPage` 获取 `ReadPageGuard`。
 *
 * ### 实现
 *
 * 您将需要实现3个主要情况。前两个相对简单：一个是当有大量可用内存时，另一个是当我们实际上
 * 不需要执行任何额外的I/O时。思考这两种情况具体涉及什么。
 *
 * 第三种情况是最棘手的，即当我们没有任何_容易_可用的内存供我们使用时。缓冲池的任务是
 * 找到它可以用来引入一个内存页面的内存，使用您之前实现的替换算法来找到可能被驱逐的帧。
 *
 * 一旦缓冲池确定了一个要驱逐的帧，可能需要进行多次I/O操作才能将我们想要的数据页面
 * 带入到该帧中。
 *
 * 与 `CheckedReadPage` 可能会有很多共享代码，所以您可能会发现创建辅助函数很有用。
 *
 * 这两个函数是这个项目的关键，所以我们不会给您比这更多的提示。祝您好运！
 *
 * TODO(P1): 添加实现。
 *
 * @param page_id 我们想要写入的页面的ID。
 * @param access_type 页面访问的类型。
 * @return std::optional<WritePageGuard> 一个可选的闩锁守卫，如果没有更多的空闲帧（内存不足）
 * 返回 `std::nullopt`，否则返回一个 `WritePageGuard`，确保对页面数据的独占和可变访问。
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 1. 在加锁之前先检查无效页面ID
  if (page_id == INVALID_PAGE_ID || page_id < 0 || page_id >= next_page_id_.load()) {
    return std::nullopt;
  }
  // 2. 获取全局锁，保护 Buffer Pool 内部数据结构
  std::unique_lock<std::mutex> lock(*bpm_latch_);

  // 3. 如果页面已经在内存中，直接增加 pin_count 并更新替换器信息
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    auto frame_id = it->second;
    auto frame = frames_[frame_id];  // frameHeader
    // 若当前页面未被固定则增加固定计数
    if (frame->pin_count_.load() == 0) {
      frame->pin_count_++;
    }
    // 记录访问信息，并设置为不可驱逐
    replacer_->RecordAccess(frame_id, access_type);
    replacer_->SetEvictable(frame_id, false);
    // 释放全局锁，防止死锁，然后获取页面的写锁
    lock.unlock();
    frame->rwlatch_.lock();
    return WritePageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
  }

  // 4. 页面不在内存中：先检查是否存在空闲帧
  if (free_frames_.empty()) {
    // 没有空闲帧则尝试驱逐一个页面
    auto victim = replacer_->Evict();
    if (!victim.has_value()) {
      return std::nullopt;
    }
    auto victim_frame_id = victim.value();
    auto old_page_id = INVALID_PAGE_ID;
    for (auto &it : page_table_) {
      if (it.second == victim_frame_id) {
        old_page_id = it.first;
        break;
      }
    }
    FlushPageUnsafe(old_page_id);
    // 从页表中移除旧页面映射，并将该帧加入空闲帧列表
    page_table_.erase(old_page_id);
    // 将驱逐的帧号放回空闲帧列表
    free_frames_.push_back(victim_frame_id);
  }

  // 5. 此时必定有空闲帧，取出空闲帧并加载新页面
  auto frame_id = free_frames_.front();
  free_frames_.pop_front();
  auto frame = frames_[frame_id];

  // 重置帧状态，并固定该页面
  frame->Reset();
  frame->pin_count_ = 1;

  // 更新页表以及替换器信息
  page_table_[page_id] = frame_id;
  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);

  // 从磁盘读取页面数据
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule(
      {.is_write_ = false, .data_ = frame->GetDataMut(), .page_id_ = page_id, .callback_ = std::move(promise)});
  

  // 6. 释放全局锁后，再对该页面加写锁
  lock.unlock();
  future.get();
  frame->rwlatch_.lock();



  return WritePageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
/**
 * @brief 获取对数据页面的可选读锁守卫。用户可以根据需要指定 `AccessType`。
 *
 * 如果无法将数据页面加载到内存中，此函数将返回 `std::nullopt`。
 *
 * 页面数据只能通过页面守卫访问。此 `BufferPoolManager` 的用户应该根据他们希望访问数据的
 * 模式获取 `ReadPageGuard` 或 `WritePageGuard`，这确保了任何数据访问都是线程安全的。
 *
 * 可以有任意数量的 `ReadPageGuard` 在不同线程中同时读取同一个数据页面。
 * 但是，所有数据访问必须是不可变的。如果用户想要改变页面的数据，他们必须使用
 * `CheckedWritePage` 获取 `WritePageGuard`。
 *
 * ### 实现
 *
 * 请参阅 `CheckedWritePage` 的实现详情。
 *
 * TODO(P1): 添加实现。
 *
 * @param page_id 我们想要读取的页面的ID。
 * @param access_type 页面访问的类型。
 * @return std::optional<ReadPageGuard> 一个可选的闩锁守卫，如果没有更多的空闲帧（内存不足）
 * 返回 `std::nullopt`，否则返回一个 `ReadPageGuard`，确保对页面数据的共享和只读访问。
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  //  对无效页面ID的检查要在获取锁之前
  if (page_id == INVALID_PAGE_ID || page_id < 0 || page_id >= next_page_id_.load()) {
    return std::nullopt;
  }

  // 加锁保护buffer pool manager内部数据结构
  std::unique_lock<std::mutex> lock(*bpm_latch_);
  // 检查页面是否在内存中
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    auto frame = frames_[frame_id];

    if (frame->pin_count_.load() == 0) {
      frame->pin_count_++;
    }

    replacer_->RecordAccess(frame_id, access_type);
    replacer_->SetEvictable(frame_id, false);

    lock.unlock();
    frame->rwlatch_.lock_shared();
    return ReadPageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
  }

  if (free_frames_.empty()) {
    // 尝试驱逐一个页面
    auto victim = replacer_->Evict();
    if (!victim.has_value()) {
      return std::nullopt;
    }
    auto victim_frame_id = victim.value();
    auto old_page_id = INVALID_PAGE_ID;
    for (auto &it : page_table_) {
      if (it.second == victim_frame_id) {
        old_page_id = it.first;
        break;
      }
    }
    FlushPageUnsafe(old_page_id);
    // 从页表中移除旧页面映射，并将该帧加入空闲帧列表
    page_table_.erase(old_page_id);
    // 将驱逐的帧号放回空闲帧列表
    free_frames_.push_back(victim_frame_id);
  }

  // 获取一个空闲帧并将新页面加载到内存中
  auto frame_id = free_frames_.front();
  free_frames_.pop_front();

  // 初始化新帧
  frames_[frame_id]->Reset();
  frames_[frame_id]->pin_count_ = 1;

  // 从磁盘读取页面数据
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({.is_write_ = false,
                             .data_ = frames_[frame_id]->GetDataMut(),
                             .page_id_ = page_id,
                             .callback_ = std::move(promise)});
  future.get();
  lock.unlock();

  frames_[frame_id]->rwlatch_.lock_shared();

  // 更新页表和替换器
  page_table_[page_id] = frame_id;
  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);
  return ReadPageGuard(page_id, frames_[frame_id], replacer_, bpm_latch_, disk_scheduler_);
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
/**
 * @brief `CheckedWritePage` 的包装器，如果内部值存在，则解包该值。
 *
 * 如果 `CheckedWritePage` 返回 `std::nullopt`，**此函数中止整个进程。**
 *
 * 此函数应**仅**用于测试和舒适的目的。如果缓冲池管理器有可能耗尽内存，
 * 则使用 `CheckedPageWrite` 允许您处理这种情况。
 *
 * 有关更多实现信息，请参阅 `CheckedPageWrite` 的文档。
 *
 * @param page_id 我们想要读取的页面的ID。
 * @param access_type 页面访问的类型。
 * @return WritePageGuard 一个页面守卫，确保对页面数据的独占和可变访问。
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);
  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }
  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
/**
 * @brief `CheckedReadPage` 的包装器，如果内部值存在，则解包该值。
 *
 * 如果 `CheckedReadPage` 返回 `std::nullopt`，**此函数中止整个进程。**
 *
 * 此函数应**仅**用于测试和人体工程学的目的。如果缓冲池管理器有可能耗尽内存，
 * 则使用 `CheckedPageWrite` 允许您处理这种情况。
 *
 * 有关更多实现信息，请参阅 `CheckedPageRead` 的文档。
 *
 * @param page_id 我们想要读取的页面的ID。
 * @param access_type 页面访问的类型。
 * @return ReadPageGuard 一个页面守卫，确保对页面数据的共享和只读访问。
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk unsafely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should not take a lock on the page in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
/**
 * @brief 不安全地将页面的数据刷新到磁盘。
 *
 * 如果页面已被修改，此函数将页面的数据写入磁盘。如果给定的页面不在内存中，
 * 此函数将返回 `false`。
 *
 * 在此函数中，您不应该对页面加锁。
 * 这意味着您应该仔细考虑何时切换 `is_dirty_` 位。
 *
 * ### 实现
 *
 * 您可能想在完成 `CheckedReadPage` 和 `CheckedWritePage` 之后再实现此函数，
 * 因为这样理解要做什么会容易得多。
 *
 * TODO(P1): 添加实现
 *
 * @param page_id 要刷新的页面的页面ID。
 * @return 如果在页表中找不到页面，则为 `false`，否则为 `true`。
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");

  // 检查页表中是否存在目标页面，如果不存在，说明该页面不在内存中，返回 false
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  // 获取目标页面对应的帧 ID 和帧头
  frame_id_t frame_id = page_table_[page_id];
  auto frame_header = frames_[frame_id];
  // 如果该帧不是脏页，则无需刷新，直接返回 true
  if (!frame_header->is_dirty_) {
    return true;
  }
  // 创建一个 promise 对象用于等待磁盘写回完成
  std::promise<bool> flush_promise = disk_scheduler_->CreatePromise();
  auto flush_future = flush_promise.get_future();

  // 将写请求提交给磁盘调度器
  disk_scheduler_->Schedule({.is_write_ = true,
                             .data_ = frame_header->GetDataMut(),
                             .page_id_ = page_id,
                             .callback_ = std::move(flush_promise)});
  // 阻塞等待写回操作完成，并获取返回结果
  if (!flush_future.get()) {
    return false;
  }

  // 写回成功后，将该帧标记为非脏页
  frame_header->is_dirty_ = false;

  return true;
}

/**
 * @brief Flushes a page's data out to disk safely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should take a lock on the page in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `Flush` in the page guards, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
/**
 * @brief 安全地将页面的数据刷新到磁盘。
 *
 * 如果页面已被修改，此函数将页面的数据写入磁盘。如果给定的页面不在内存中，
 * 此函数将返回 `false`。
 *
 * 在此函数中，您应该对页面加锁，以确保将一致的状态刷新到磁盘。
 *
 * ### 实现
 *
 * 您可能想在完成 `CheckedReadPage`、`CheckedWritePage` 和页面守卫中的 `Flush` 之后
 * 再实现此函数，因为这样理解要做什么会容易得多。
 *
 * TODO(P1): 添加实现
 *
 * @param page_id 要刷新的页面的页面ID。
 * @return 如果在页表中找不到页面，则为 `false`，否则为 `true`。
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  //  对缓冲池内部数据结构上锁，确保对页表等数据结构的互斥访问
  std::scoped_lock lock(*bpm_latch_);
  return FlushPageUnsafe(page_id);
}

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 *
 * You should not take locks on the pages in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
/**
 * @brief 不安全地将内存中的所有页面数据刷新到磁盘。
 *
 * 在此函数中，您不应该对页面加锁。
 * 这意味着您应该仔细考虑何时切换 `is_dirty_` 位。
 *
 * ### 实现
 *
 * 您可能想在完成 `CheckedReadPage`、`CheckedWritePage` 和 `FlushPage` 之后
 * 再实现此函数，因为这样理解要做什么会容易得多。
 *
 * TODO(P1): 添加实现
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 遍历页表中的每个页面
  for (auto &entry : page_table_) {
    FlushPageUnsafe(entry.first);
  }
}

/**
 * @brief Flushes all page data that is in memory to disk safely.
 *
 * You should take locks on the pages in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
/**
 * @brief 安全地将内存中的所有页面数据刷新到磁盘。
 *
 * 在此函数中，您应该对页面加锁，以确保将一致的状态刷新到磁盘。
 *
 * ### 实现
 *
 * 您可能想在完成 `CheckedReadPage`、`CheckedWritePage` 和 `FlushPage` 之后
 * 再实现此函数，因为这样理解要做什么会容易得多。
 *
 * TODO(P1): 添加实现
 */
void BufferPoolManager::FlushAllPages() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  //  首先锁住缓冲池管理器，确保遍历页表时数据结构不会被并发修改
  std::scoped_lock lock(*bpm_latch_);
  // 遍历页表中所有页面
  for (auto &entry : page_table_) {
    FlushPage(entry.first);
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
 */
/**
 * @brief 检索页面的引用计数。如果页面不存在于内存中，返回 `std::nullopt`。
 *
 * 此函数是线程安全的。调用者可以在多线程环境中调用此函数，其中多个线程
 * 访问同一页面。
 *
 * 此函数旨在用于测试目的。如果此函数实现不正确，它肯定会导致测试套件和自动评分器
 * 出现问题。
 *
 * # 实现
 *
 * 我们将使用此函数来测试您的缓冲池管理器是否正确管理引用计数。由于 `FrameHeader` 中的
 * `pin_count_` 字段是原子类型，您不需要对持有我们想要查看的页面的帧加锁。相反，您可以
 * 简单地使用原子 `load` 安全地加载存储的值。但是，您仍然需要获取缓冲池闩锁。
 *
 * 再次，如果您不熟悉原子类型，请参阅C++官方文档
 * [这里](https://en.cppreference.com/w/cpp/atomic/atomic)。
 *
 * TODO(P1): 添加实现
 *
 * @param page_id 我们想要获取引用计数的页面的页面ID。
 * @return std::optional<size_t> 如果页面存在，则为引用计数，否则为 `std::nullopt`。
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  //  加锁保护缓冲池内部数据结构，确保对页表的访问不会发生竞争
  std::scoped_lock lock(*bpm_latch_);

  // 检查页表中是否存在目标页面
  if (page_table_.find(page_id) == page_table_.end()) {
    // 如果页面不在内存中，则返回空值
    return std::nullopt;
  }

  // 获取该页面对应的帧 ID
  frame_id_t frame_id = page_table_[page_id];

  // 通过帧对象的原子变量 pin_count_ 安全读取当前固定计数并返回
  return frames_[frame_id]->pin_count_.load();
}

}  // namespace bustub
