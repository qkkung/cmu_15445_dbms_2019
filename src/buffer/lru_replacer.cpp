/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {
  this->size = 0;
  this->head = nullptr;
  this->tail = nullptr;
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  std::lock_guard<std::mutex> guard(this->mutex);

  erase(value);
  insertAtHead(value);
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  std::lock_guard<std::mutex> guard(this->mutex);
  if (this->tail == nullptr) {
    return false;
  }
  value = this->tail->value;
  if (this->tail == this->head) {
    this->head = nullptr;
    this->tail = nullptr;
    this->index.erase(value);
    return true;
  }
  this->tail->pre->next = nullptr;
  this->tail = this->tail->pre;
  this->index.erase(value);
  this->size--;
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  std::lock_guard<std::mutex> guard(this->mutex);
  return erase(value);
}

template <typename T> size_t LRUReplacer<T>::Size() { return this->size; }

template <typename T> void LRUReplacer<T>::insertAtHead(const T& value) {
  std::shared_ptr<DLinkedNode> ptr = std::make_shared<DLinkedNode>(value);
  this->index[value] = ptr;

  ptr->pre = nullptr;
  ptr->next = this->head;
  if (this->head != nullptr) {
    this->head->pre = ptr;
  }
  this->head = ptr;
  if (this->tail == nullptr) {
    this->tail = ptr;
  }

  this->size++;
  return;
}

/*
 * This function is not applied lock, 
 * which will be convenient for other function to invoke 
 */
template <typename T> bool LRUReplacer<T>::erase(const T& value) {
  auto iter = this->index.find(value);
  if (iter == this->index.end()) {
    return false;
  }
	
  auto ptr = iter->second;
  if (ptr == this->head) {
    this->head = ptr->next;
  }
  if (ptr == this->tail) {
    this->tail = ptr->pre;
  }
  if (ptr->pre != nullptr) {
    ptr->pre->next = ptr->next;
  }
  if (ptr->next != nullptr) {
    ptr->next->pre = ptr->pre;
  }
  this->index.erase(value);
  this->size--;
  return true;
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
