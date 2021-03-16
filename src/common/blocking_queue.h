#ifndef MORPH_COMMON_BLOCKINGQUEUE_H
#define MORPH_COMMON_BLOCKINGQUEUE_H

#include <mutex>
#include <condition_variable>
#include <deque>
#include <common/nocopy.h>

namespace morph {

template <typename T>
class BlockingQueue: NoCopy {
 public:
  BlockingQueue():
    mutex(),
    queue()
  {}

  void push(const T &x) {
    std::lock_guard<std::mutex> lock(mutex);
    queue.push_back(x);
    cv.notify_one();
  }

  void push(T &&x) {
    std::lock_guard<std::mutex> lock(mutex);
    queue.push_back(std::move(x));
    cv.notify_one();
  }

  T pop() {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [this]() {
      return !queue.empty();
    });
    assert(!queue.empty());
    T front(std::move(queue.front()));
    queue.pop_front();
    return front;
  }

  bool try_pop(T &item) {
    std::unique_lock<std::mutex> lock(mutex);
    if (queue.empty()) {
      return false;
    }
    item = std::move(queue.front());
    queue.pop_front();
    return true;
  }

  size_t size() const {
    std::lock_guard<std::mutex> lock(mutex);
    return queue.size();
  }

  bool empty() const {
    std::lock_guard<std::mutex> lock(mutex);
    return queue.empty();
  }

 private:
  mutable std::mutex mutex;
  std::condition_variable cv;
  std::deque<T> queue;
};

}

#endif
