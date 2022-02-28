#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) 
  :globalDepth(0), bucketMaxSize(size), numBuckets(1) {
  bucketTable.push_back(std::make_shared<Bucket>(0));
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) const {
  return std::hash<K>()(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  return globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  return bucketTable[bucket_id]->localDepth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  return numBuckets;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
    std::lock_guard<std::mutex> guard(mutex);
	
  int index = getBucketIndex(key);
  std::shared_ptr<Bucket> bucketPtr = bucketTable[index];
  if (bucketPtr != nullptr && bucketPtr->items.find(key) != bucketPtr->items.end()) {
    value = bucketPtr->items[key];
    return true;	
  }	 
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  std::lock_guard<std::mutex> guard(mutex);
	
  int index = getBucketIndex(key);
  std::shared_ptr<Bucket> bucketPtr = bucketTable[index];
  if (bucketPtr != nullptr && bucketPtr->items.find(key) != bucketPtr->items.end()) {
    bucketPtr->items.erase(key);
    return true; 
  }
  return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> guard(mutex);

  int index = getBucketIndex(key);
  std::shared_ptr<Bucket> targetBucketPtr = bucketTable[index];
	
  while (targetBucketPtr->items.size() == bucketMaxSize) {
    if (targetBucketPtr->localDepth == globalDepth) {
      bucketTable.resize(bucketTable.size() * 2);
      int halfSize = bucketTable.size() / 2;
      for (int tableIndex = 0; tableIndex < halfSize; tableIndex++) {
        bucketTable[tableIndex + halfSize] = bucketTable[tableIndex]; 
      }
      globalDepth++;
    }
    std::shared_ptr<Bucket> zeroBucketPtr = std::make_shared<Bucket>(targetBucketPtr->localDepth + 1);
    std::shared_ptr<Bucket> oneBucketPtr = std::make_shared<Bucket>(targetBucketPtr->localDepth + 1);
    this->numBuckets++;
    int mask = 1<<(zeroBucketPtr->localDepth - 1);
		
    for (auto& item : targetBucketPtr->items) {
      if (getBucketIndex(item.first) & mask) {
        oneBucketPtr->items.insert(item);
      } else {
        zeroBucketPtr->items.insert(item);
      }
    }
		
    for (size_t i = 0; i < bucketTable.size(); i++) {
      if (bucketTable[i] == targetBucketPtr) {
        if (i & mask) {
          bucketTable[i] = oneBucketPtr;
        } else {
          bucketTable[i] = zeroBucketPtr;
        }
      }
    }
    targetBucketPtr = bucketTable[getBucketIndex(key)];
  }
  targetBucketPtr->items[key] = value;  
}

template <typename K, typename V>
size_t ExtendibleHash<K, V>::getBucketIndex(const K& key) const {
  return HashKey(key) & ((1<<globalDepth) - 1);
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
