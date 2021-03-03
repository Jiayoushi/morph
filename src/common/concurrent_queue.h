// Adapted from "c++ concurrency in action"

#ifndef MORPH_COMMON_BLOCKING_QUEUE_H
#define MORPH_COMMON_BLOCKING_QUEUE_H

#include <mutex>
#include <condition_variable>
#include <deque>
#include <common/nocopy.h>

namespace morph {


template <typename T>
class ConcurrentQueue: NoCopy {
 public:
  ConcurrentQueue():
    head(new Node), tail(head.get())
  {}

  std::shared_ptr<T> peek() {
    std::lock_guard<std::mutex> head_lock(head_mutex);
    if (head.get() == get_tail()) {
      return std::shared_ptr<T>();
    }
    return head->item;
  }

  void push(T item) {
    std::shared_ptr<T> new_item(std::make_shared<T>(std::move(item)));
    put_at_tail(new_item);
  }

  void push(std::unique_ptr<T> item) {
    std::shared_ptr<T> new_item = std::move(item);
    put_at_tail(new_item);
  }

  template  <typename...Args> 
  void emplace(Args &&...args) {
    std::shared_ptr<T> new_item(std::make_shared<T>(args...));
    put_at_tail(new_item);
  }

  std::shared_ptr<T> wait_and_pop() {
    std::unique_ptr<Node> const old_head = wait_pop_head();
    return old_head->item;
  }

  void wait_and_pop(T **item) {
    std::unique_ptr<Node> const old_head = wait_pop_head(item);
  }

  bool empty() {
    std::lock_guard<std::mutex> head_lock(head_mutex);
    return head.get() == get_tail();
  }

 private:
  struct Node {
    std::shared_ptr<T> item;
    std::unique_ptr<Node> next;
  };
  
  void put_at_tail(std::shared_ptr<T> new_item);

  Node *get_tail() {
    std::lock_guard<std::mutex> tail_lock(tail_mutex);
    return tail;
  }

  std::unique_ptr<Node> pop_head() {
    std::unique_ptr<Node> old_head = std::move(head);
    head = std::move(old_head->next);
    return old_head;
  }

  std::unique_lock<std::mutex> wait_for_data() {
    std::unique_lock<std::mutex> head_lock(head_mutex);
    item_cv.wait(head_lock, [&]{return head.get() != get_tail();});
    return std::move(head_lock);
  }

  std::unique_ptr<Node> wait_pop_head() {
    std::unique_lock<std::mutex> head_lock(wait_for_data());
    return pop_head();
  }

  std::unique_ptr<Node> wait_pop_head(T **item) {
    std::unique_lock<std::mutex> head_lock(wait_for_data());
    *item = head->item.get();
    return pop_head();
  }

  std::mutex head_mutex;
  std::unique_ptr<Node> head;
  std::mutex tail_mutex;
  Node *tail;
  std::condition_variable item_cv;
};

template <typename T>
void ConcurrentQueue<T>::put_at_tail(std::shared_ptr<T> new_item) {
  std::unique_ptr<Node> p(new Node);

  {
    std::lock_guard<std::mutex> tail_lock(tail_mutex);
    tail->item = new_item;
    Node *const new_tail = p.get();
    tail->next = std::move(p);
    tail = new_tail;
  }
  item_cv.notify_one();
}

}

#endif