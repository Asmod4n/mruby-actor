#pragma once

#include <atomic>
#include <cstdint>
#include <new>
#include <thread>
#include <utility>

// MpscQueue: Vyukov-style intrusive multi-producer / single-consumer FIFO.
//
//   - push() is wait-free: one CAS-free atomic exchange + one release store.
//   - try_pop() is single-consumer; lock-free with one false-empty case for
//     in-flight pushes (caller is expected to retry / wait).
//   - pop_blocking() parks the consumer with std::atomic::wait until a
//     producer notifies via std::atomic::notify_one.
//
// Reference: D. Vyukov, "Non-intrusive MPSC node-based queue".
template <typename T>
class MpscQueue {
public:
  MpscQueue() : head_(&stub_), tail_(&stub_) {}

  MpscQueue(const MpscQueue&)            = delete;
  MpscQueue& operator=(const MpscQueue&) = delete;

  ~MpscQueue() {
    T tmp;
    while (try_pop(tmp)) {}
  }

  void push(T value) {
    Node* n    = new Node(std::move(value));
    Node* prev = head_.exchange(n, std::memory_order_acq_rel);
    prev->next.store(n, std::memory_order_release);

    seq_.fetch_add(1, std::memory_order_release);
    seq_.notify_one();
  }

  bool try_pop(T& out) {
    Node* tail = tail_;
    Node* next = tail->next.load(std::memory_order_acquire);

    if (tail == &stub_) {
      if (next == nullptr) return false;
      tail_ = next;
      tail  = next;
      next  = next->next.load(std::memory_order_acquire);
    }

    if (next != nullptr) {
      tail_ = next;
      out   = std::move(tail->value);
      delete tail;
      return true;
    }

    Node* head = head_.load(std::memory_order_acquire);
    if (tail != head) {
      // Producer mid-push between exchange and link.
      return false;
    }

    // Drained: re-enqueue the stub so future producers have a sentinel
    // to link to.
    stub_.next.store(nullptr, std::memory_order_relaxed);
    Node* prev = head_.exchange(&stub_, std::memory_order_acq_rel);
    prev->next.store(&stub_, std::memory_order_release);

    next = tail->next.load(std::memory_order_acquire);
    if (next != nullptr) {
      tail_ = next;
      out   = std::move(tail->value);
      delete tail;
      return true;
    }
    return false;
  }

  T pop_blocking() {
    T out;
    while (true) {
      uint32_t observed = seq_.load(std::memory_order_acquire);
      if (try_pop(out)) return out;
      seq_.wait(observed, std::memory_order_acquire);
    }
  }

  // Wake all parked consumers. Useful for unblocking pop_blocking()
  // from a shutdown path even when the queue is empty.
  void wake_all() {
    seq_.fetch_add(1, std::memory_order_release);
    seq_.notify_all();
  }

private:
  struct Node {
    std::atomic<Node*> next{nullptr};
    T                  value{};
    Node()                            = default;
    explicit Node(T v) : value(std::move(v)) {}
  };

  static constexpr std::size_t kCacheLine = 64;

  alignas(kCacheLine) std::atomic<Node*>    head_;
  alignas(kCacheLine) Node*                 tail_;
  Node                                       stub_;
  alignas(kCacheLine) std::atomic<uint32_t> seq_{0};
};
