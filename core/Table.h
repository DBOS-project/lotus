//
// Created by Yi Lu on 7/18/18.
//

#pragma once
#include <thread>
#include "benchmark/tpcc/Schema.h"
#include "common/ClassOf.h"
#include "common/Encoder.h"
#include "common/HashMap.h"
#include "common/StringPiece.h"

static thread_local uint64_t tid = std::numeric_limits<uint64_t>::max();
extern bool do_tid_check;
static std::hash<std::thread::id> tid_hasher;
namespace star {

void tid_check() {
  if (do_tid_check) {
    if (tid == std::numeric_limits<uint64_t>::max()) {
      tid = tid_hasher(std::this_thread::get_id());
    } else {
      DCHECK(tid_hasher(std::this_thread::get_id()) == tid);
    }
  }
}

class ITable {
public:
  using MetaDataType = std::atomic<uint64_t>;

  virtual ~ITable() = default;

  virtual std::tuple<MetaDataType *, void *> search(const void *key) = 0;

  virtual void *search_value(const void *key) = 0;

  virtual MetaDataType &search_metadata(const void *key) = 0;

  virtual void insert(const void *key, const void *value) = 0;

  virtual void update(const void *key, const void *value) = 0;

  virtual void deserialize_value(const void *key, StringPiece stringPiece) = 0;

  virtual void serialize_value(Encoder &enc, const void *value) = 0;

  virtual std::size_t key_size() = 0;

  virtual std::size_t value_size() = 0;

  virtual std::size_t field_size() = 0;

  virtual std::size_t tableID() = 0;

  virtual std::size_t partitionID() = 0;
};

template <std::size_t N, class KeyType, class ValueType>
class Table : public ITable {
public:
  using MetaDataType = std::atomic<uint64_t>;

  virtual ~Table() override = default;

  Table(std::size_t tableID, std::size_t partitionID)
      : tableID_(tableID), partitionID_(partitionID) {}

  std::tuple<MetaDataType *, void *> search(const void *key) override {
    tid_check();
    const auto &k = *static_cast<const KeyType *>(key);
    auto &v = map_[k];
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value(const void *key) override {
    tid_check();
    const auto &k = *static_cast<const KeyType *>(key);
    return &std::get<1>(map_[k]);
  }

  MetaDataType &search_metadata(const void *key) override {
    tid_check();
    const auto &k = *static_cast<const KeyType *>(key);
    return std::get<0>(map_[k]);
  }

  void insert(const void *key, const void *value) override {
    tid_check();
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    bool ok = map_.contains(k);
    DCHECK(ok == false);
    auto &row = map_[k];
    std::get<0>(row).store(0);
    std::get<1>(row) = v;
  }

  void update(const void *key, const void *value) override {
    tid_check();
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    auto &row = map_[k];
    std::get<1>(row) = v;
  }

  void deserialize_value(const void *key, StringPiece stringPiece) override {
    tid_check();
    std::size_t size = stringPiece.size();
    const auto &k = *static_cast<const KeyType *>(key);
    auto &row = map_[k];
    auto &v = std::get<1>(row);

    Decoder dec(stringPiece);
    dec >> v;

    DCHECK(size - dec.size() == ClassOf<ValueType>::size());
  }

  void serialize_value(Encoder &enc, const void *value) override {
    tid_check();
    std::size_t size = enc.size();
    const auto &v = *static_cast<const ValueType *>(value);
    enc << v;

    DCHECK(enc.size() - size == ClassOf<ValueType>::size());
  }

  std::size_t key_size() override { tid_check(); return sizeof(KeyType); }

  std::size_t value_size() override { tid_check(); return sizeof(ValueType); }

  std::size_t field_size() override { tid_check(); return ClassOf<ValueType>::size(); }

  std::size_t tableID() override { tid_check(); return tableID_; }

  std::size_t partitionID() override { tid_check(); return partitionID_; }

private:
  HashMap<N, KeyType, std::tuple<MetaDataType, ValueType>> map_;
  std::size_t tableID_;
  std::size_t partitionID_;
};
template <class KeyType, class ValueType>
class HStoreTable : public ITable {
public:
  using MetaDataType = std::atomic<uint64_t>;

  virtual ~HStoreTable() override = default;

  HStoreTable(std::size_t tableID, std::size_t partitionID)
      : tableID_(tableID), partitionID_(partitionID) {}

  std::tuple<MetaDataType *, void *> search(const void *key) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto &v = map_[k];
    return std::make_tuple(nullptr, &(v));
  }

  void *search_value(const void *key) override {
    const auto &k = *static_cast<const KeyType *>(key);
    return &map_[k];
  }

  MetaDataType &search_metadata(const void *key) override {
    static MetaDataType v;
    return v;
  }

  void insert(const void *key, const void *value) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    bool ok = map_.contains(k);
    DCHECK(ok == false);
    auto &row = map_[k];
    row = v;
  }

  void update(const void *key, const void *value) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    auto &row = map_[k];
    row = v;
  }

  void deserialize_value(const void *key, StringPiece stringPiece) override {

    std::size_t size = stringPiece.size();
    const auto &k = *static_cast<const KeyType *>(key);
    auto &row = map_[k];
    auto &v = row;

    Decoder dec(stringPiece);
    dec >> v;

    DCHECK(size - dec.size() == ClassOf<ValueType>::size());
  }

  void serialize_value(Encoder &enc, const void *value) override {

    std::size_t size = enc.size();
    const auto &v = *static_cast<const ValueType *>(value);
    enc << v;

    DCHECK(enc.size() - size == ClassOf<ValueType>::size());
  }

  std::size_t key_size() override { return sizeof(KeyType); }

  std::size_t value_size() override { return sizeof(ValueType); }

  std::size_t field_size() override { return ClassOf<ValueType>::size(); }

  std::size_t tableID() override { return tableID_; }

  std::size_t partitionID() override { return partitionID_; }

private:
  UnsafeHashMap<KeyType, ValueType> map_;
  std::size_t tableID_;
  std::size_t partitionID_;
};
} // namespace star
