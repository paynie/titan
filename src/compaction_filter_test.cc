#include "db_impl.h"
#include "test_util/testharness.h"
#include "rocksdb/compaction_filter.h"
#include "titan_logging.h"
#include "util/mutexlock.h"

#include <time.h>
#include <stdio.h>
#include <iostream>
#include <string.h>
#include "ttl.h"

namespace rocksdb {
namespace titandb {

class TestCompactionFilter : public CompactionFilter {
 public:
  explicit TestCompactionFilter(uint64_t min_blob_size)
      : min_blob_size_(min_blob_size) {}

  const char *Name() const override { return "DeleteCompactionFilter"; }

  bool Filter(int level, const Slice &key, const Slice &value,
              std::string * /*&new_value*/,
              bool * /*value_changed*/) const override {
    AssertValue(key, value);
    return !value.starts_with("remain");
  }

 private:
  void AssertValue(const Slice &key, const Slice &value) const {
    if (key.ToString() == "mykey") {
      ASSERT_EQ(value.ToString(), "myvalue");
    }
    if (key.ToString() == "bigkey") {
      ASSERT_EQ(value.ToString(), std::string(min_blob_size_ + 1, 'v'));
    }
    if (key.starts_with("skip")) {
      ASSERT_EQ(value, Slice());
    }
  }

  uint64_t min_blob_size_;
};

class TTLCompactionFilter: public CompactionFilter {
  public:
  explicit TTLCompactionFilter(uint64_t min_blob_size)
      : min_blob_size_(min_blob_size) {}

  const char *Name() const override { return "TTLCompactionFilter"; }
      
  bool FilterBlob(int level, const Slice &key, const Slice &value,
              std::string * /*&new_value*/,
              bool * /*value_changed*/) const {
    // Parse ttl from value and append it to key index
    const char* pd = value.data_;
    int32_t len = value.size(); 
    uint64_t ttl = ParseTTL(pd, len);
    std::cout << "value len " << len <<" in compaction, value ttl is " << ttl << std::endl;

    // Get current ttl
    uint64_t tss = static_cast<uint64_t>(time(NULL));

    std::cout << "decode current ts = " << tss << ", ttl = " << ttl << std::endl;
    

    BlobIndex blob_index;
    Slice original_value(value.data());
    Status s = blob_index.DecodeFrom(&original_value);

    // Get current ts
    uint64_t ts = static_cast<uint64_t>(std::time(0));

    std::cout << "current ts = " << ts << ", ttl = " << blob_index.ttl << std::endl;

    if(blob_index.ttl != 0 && blob_index.ttl < ts) {
      std::cout << "removed" << std::endl;
      // has ttl and ttl < current ts, need remove
      return true;
    }

    return false;
  }

    bool FilterValue(int level, const Slice &key, const Slice &value,
              std::string * /*&new_value*/,
              bool * /*value_changed*/) const {
    // Parse ttl from value and append it to key index
    const char* pd = value.data_;
    int32_t len = value.size(); 
    uint64_t ttl = ParseTTL(pd, len);
    std::cout << "in compaction, value ttl is " << ttl << std::endl;

    // Get current ttl
    uint64_t tss = static_cast<uint64_t>(time(NULL));

    std::cout << "decode current ts = " << tss << ", ttl = " << ttl << std::endl;
    
    if(ttl != 0 && ttl < tss) {
      return true;
    } else {
      return false;
    }
  }

  Decision FilterV3(int level, const Slice& key,
                            SequenceNumber /*seqno*/, ValueType value_type,
                            const Slice& existing_value, std::string* new_value,
                            std::string* skip_until) const override {
    switch (value_type) {
      case ValueType::kValue: {
        bool value_changed = false;
        bool rv = FilterValue(level, key, existing_value, new_value, &value_changed);
        if (rv) {
          return Decision::kRemove;
        }
        return value_changed ? Decision::kChangeValue : Decision::kKeep;
      }

      case ValueType::kBlobIndex: {
        bool value_changed = false;
        bool rv = FilterBlob(level, key, existing_value, new_value, &value_changed);
        if (rv) {
          return Decision::kRemove;
        }
        return value_changed ? Decision::kChangeValue : Decision::kKeep;
      }

      case ValueType::kMergeOperand: {
        bool rv = FilterMergeOperand(level, key, existing_value);
        return rv ? Decision::kRemove : Decision::kKeep;
      }
    }
    assert(false);
    return Decision::kKeep;
 
 }

  private:
  uint64_t min_blob_size_;
};

class TitanCompactionFilterTest : public testing::Test {
 public:
  TitanCompactionFilterTest() : dbname_(test::TmpDir()) {
    options_.dirname = dbname_ + "/titandb";
    options_.create_if_missing = true;
    options_.disable_background_gc = true;
    options_.disable_auto_compactions = true;
    options_.compaction_filter =
        new TTLCompactionFilter(options_.min_blob_size);

    std::cout << "min_blob_size = " << options_.min_blob_size << std::endl;

    DeleteDir(options_.dirname);
    DeleteDir(dbname_);
  }

  ~TitanCompactionFilterTest() override {
    Close();
    delete options_.compaction_filter;
    DeleteDir(options_.dirname);
    DeleteDir(dbname_);
  }

  static void DeleteDir(const std::string &dirname) {
    Env *env = Env::Default();
    std::vector<std::string> filenames;
    env->GetChildren(dirname, &filenames);

    for (auto &fname : filenames) {
      env->DeleteFile(dirname + "/" + fname);
    }
    env->DeleteDir(dirname);
  }

  void Open() {
    ASSERT_OK(TitanDB::Open(options_, dbname_, &db_));
    db_impl_ = reinterpret_cast<TitanDBImpl *>(db_);
  }

  void Close() {
    if (!db_) return;

    ASSERT_OK(db_->Close());
    delete db_;
    db_ = nullptr;
  }

  Status Get(const std::string &key, std::string *value) {
    ReadOptions ropts;
    return db_->Get(ropts, key, value);
  }

  Status Put(const std::string &key, const std::string &value) {
    WriteOptions wopts;
    return db_->Put(wopts, key, value);
  }

  std::string GetBigValue() {
    return std::string(options_.min_blob_size + 1, 'v');
  }

  void CompactAll() {
    CompactRangeOptions copts;
    ASSERT_OK(db_->CompactRange(copts, nullptr, nullptr));
  }

 protected:
  std::string dbname_;
  TitanOptions options_;
  TitanDB *db_{nullptr};
  TitanDBImpl *db_impl_{nullptr};
};


TEST_F(TitanCompactionFilterTest, CompactTTL) {
  options_.skip_value_in_compaction_filter = true;
  Open();

  for(int index = 0; index < 1; index++) {
    for(int i = 0; i < 10; i++) {
      uint64_t ts = static_cast<uint64_t>(time(NULL));
      uint64_t ttl = ts + 5;
      char key[64];
      sprintf(key, "key_%d", i);

      std::string value("v", 10240);
      
      const char* p_value = value.c_str();
      int32_t value_size = value.size();

      char* p_value_ttl = new char[value_size + 8];
      memcpy(p_value_ttl, p_value, value_size);
      longToBigBytes(ttl, p_value_ttl + value_size, 8);
      std::string new_value(p_value_ttl, value_size + 8);
      
      ASSERT_OK(db_->Put(WriteOptions(), key, new_value));
      delete[] p_value_ttl;
    }

    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  std::cout << "before compaction" << std::endl;

  // Get value
  std::string value1;
  ASSERT_TRUE(!db_->Get(ReadOptions(), "key_0", &value1).IsNotFound());

  sleep(10);

  std::cout << "start compaction" << std::endl; 
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std::string value2;
  ASSERT_TRUE(db_->Get(ReadOptions(), "key_0", &value2).IsNotFound());
}

/*
TEST_F(TitanCompactionFilterTest, CompactNormalValue) {
  Open();

  Status s = Put("mykey", "myvalue");
  ASSERT_OK(s);

  std::string value;
  s = Get("mykey", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "myvalue");

  CompactAll();

  s = Get("mykey", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(TitanCompactionFilterTest, CompactBlobValue) {
  Open();

  std::string value = GetBigValue();
  ASSERT_GT(value.length(), options_.min_blob_size);
  Status s = Put("bigkey", value);
  ASSERT_OK(s);

  std::string value1;
  s = Get("bigkey", &value1);
  ASSERT_OK(s);
  ASSERT_EQ(value1, value);

  CompactAll();

  s = Get("bigkey", &value1);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(TitanCompactionFilterTest, CompactUpdateValue) {
  options_.blob_file_discardable_ratio = 0.01;
  options_.min_blob_size = 1;
  options_.target_file_size_base = 1;
  Open();

  ASSERT_OK(db_->Put(WriteOptions(), "update-key", "remain1"));
  ASSERT_OK(db_->Put(WriteOptions(), "update-another-key", "remain2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "update-key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  uint32_t cf_id = db_->DefaultColumnFamily()->GetID();
  ASSERT_OK(db_impl_->TEST_StartGC(cf_id));
  ASSERT_OK(db_impl_->TEST_PurgeObsoleteFiles());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "update-key", &value).IsNotFound());
  ASSERT_OK(db_->Get(ReadOptions(), "update-another-key", &value));
  ASSERT_EQ(value, "remain2");
}

TEST_F(TitanCompactionFilterTest, CompactSkipValue) {
  options_.skip_value_in_compaction_filter = true;
  Open();

  ASSERT_OK(db_->Put(WriteOptions(), "skip-key", "skip-value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "skip-key", &value).IsNotFound());
}

TEST_F(TitanCompactionFilterTest, FilterNewColumnFamily) {
  options_.skip_value_in_compaction_filter = true;
  Open();
  TitanCFDescriptor desc("last_summer", options_);
  ColumnFamilyHandle *handle = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(desc, &handle));

  ASSERT_OK(db_->Put(WriteOptions(), handle, "skip-key", "skip-value"));
  ASSERT_OK(db_->Flush(FlushOptions(), handle));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), handle, nullptr, nullptr));

  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), handle, "skip-key", &value).IsNotFound());
  ASSERT_OK(db_->DestroyColumnFamilyHandle(handle));
}
*/

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}