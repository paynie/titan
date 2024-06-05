#pragma once

#include <string>
#include <utility>

#include "db_impl.h"
#include "rocksdb/compaction_filter.h"
#include "titan_logging.h"
#include "util/mutexlock.h"
#include "ttl.h"
#include <iostream>

namespace rocksdb {
namespace titandb {

class TitanCompactionFilter final : public CompactionFilter {
 public:
  TitanCompactionFilter(TitanDBImpl *db, const std::string &cf_name,
                        const CompactionFilter *original,
                        std::unique_ptr<CompactionFilter> &&owned_filter,
                        std::shared_ptr<BlobStorage> blob_storage,
                        bool skip_value,
                        bool enable_ttl)
      : db_(db),
        cf_name_(cf_name),
        blob_storage_(std::move(blob_storage)),
        original_filter_(original),
        owned_filter_(std::move(owned_filter)),
        skip_value_(skip_value),
        enable_ttl_(enable_ttl),
        filter_name_(std::string("TitanCompactionfilter.")
                         .append(original_filter_->Name())) {
    assert(blob_storage_ != nullptr);
    assert(original_filter_ != nullptr);

    std::cout << "paynie add  in  TitanCompactionFilter" << std::endl;
    TITAN_LOG_INFO(db_->db_options_.info_log,
                    "titian filter name = %s, original filter name = %s", filter_name_.c_str(), original_filter_->Name());
  }

  const char *Name() const override { return filter_name_.c_str(); }

  bool IsStackedBlobDbInternalCompactionFilter() const override { return true; }

  Decision UnsafeFilter(int level, const Slice &key, SequenceNumber seqno,
                        ValueType value_type, const Slice &value,
                        std::string *new_value,
                        std::string *skip_until) const override {

    Status s;
    Slice user_key = key;
    TITAN_LOG_INFO(db_->db_options_.info_log, "In UnsafeFilter");

    // Since IsStackedBlobDbInternalCompactionFilter was implemented as true,
    // the key is an internal key when value_type is kBlobIndex, which is caused
    // by a hack in RocksDB.
    if (value_type == kBlobIndex) {
      ParsedInternalKey ikey;
      s = ParseInternalKey(key, &ikey, false /*log_err_key*/);
      if (s.ok()) {
        user_key = ikey.user_key;
      } else {
        TITAN_LOG_ERROR(db_->db_options_.info_log,
                        "[%s] Unable to parse internal key", cf_name_.c_str());
        {
          MutexLock l(&db_->mutex_);
          db_->SetBGError(s);
        }
        return Decision::kKeep;
      }
    }

    if (enable_ttl_) {
      TITAN_LOG_INFO(db_->db_options_.info_log, "In UnsafeFilter, enable_ttl_");
      if (skip_value_) {
        TITAN_LOG_INFO(db_->db_options_.info_log, "In UnsafeFilter, skip_value_");
        return original_filter_->UnsafeFilter(level, user_key, seqno, value_type,
                                              Slice(), new_value, skip_until);
      }
      if (value_type != kBlobIndex) {
        TITAN_LOG_INFO(db_->db_options_.info_log, "In UnsafeFilter, value_type != kBlobIndex");
        return original_filter_->UnsafeFilter(level, user_key, seqno, value_type,
                                              value, new_value, skip_until);
      }
      TITAN_LOG_INFO(db_->db_options_.info_log, "In UnsafeFilter, value_type=kBlobIndex");
      const char* pd = value.data_;
      int32_t len = value.size();
      uint64_t ttl = ParseTTL(pd, len);

      BlobIndex blob_index;
      Slice original_value(value.data(), len);
      s = blob_index.DecodeFrom(&original_value);

      if (!s.ok()) {
        TITAN_LOG_ERROR(db_->db_options_.info_log,
                        "[%s] Unable to decode blob index", cf_name_.c_str());
        // TODO(yiwu): Better to fail the compaction as well, but current
        // compaction filter API doesn't support it.
        {
          MutexLock l(&db_->mutex_);
          db_->SetBGError(s);
        }
        // Unable to decode blob index. Keeping the value.
        return Decision::kKeep;
      }

      TITAN_LOG_INFO(db_->db_options_.info_log, "ttl = %lu, ttl in blob index = %lu", ttl, blob_index.ttl);

      if(blob_index.ttl < ttl) {
        blob_index.ttl = ttl;
      }

      // Get current ts
      uint64_t ts = static_cast<uint64_t>(std::time(0));

      if(blob_index.ttl != 0 && blob_index.ttl < ts) {
        TITAN_LOG_INFO(db_->db_options_.info_log, "Remove, ts = %lu, ttl = %lu", ts, blob_index.ttl);
        // has ttl and ttl < current ts, need remove
        return Decision::kRemove;
      }

      return Decision::kKeep;
    } else {
      TITAN_LOG_INFO(db_->db_options_.info_log, "In UnsafeFilter, not enable_ttl_");
      if (skip_value_) {
        return original_filter_->UnsafeFilter(level, user_key, seqno, value_type,
                                              Slice(), new_value, skip_until);
      }
      if (value_type != kBlobIndex) {
        return original_filter_->UnsafeFilter(level, user_key, seqno, value_type,
                                              value, new_value, skip_until);
      }

      BlobIndex blob_index;
      Slice original_value(value.data());
      s = blob_index.DecodeFrom(&original_value);
      if (!s.ok()) {
        TITAN_LOG_ERROR(db_->db_options_.info_log,
                        "[%s] Unable to decode blob index", cf_name_.c_str());
        // TODO(yiwu): Better to fail the compaction as well, but current
        // compaction filter API doesn't support it.
        {
          MutexLock l(&db_->mutex_);
          db_->SetBGError(s);
        }
        // Unable to decode blob index. Keeping the value.
        return Decision::kKeep;
      }

      BlobRecord record;
      PinnableSlice buffer;
      ReadOptions read_options;
      s = blob_storage_->Get(read_options, blob_index, &record, &buffer);

      if (s.IsCorruption()) {
        // Could be cause by blob file beinged GC-ed, or real corruption.
        // TODO(yiwu): Tell the two cases apart.
        return Decision::kKeep;
      } else if (s.ok()) {
        auto decision = original_filter_->UnsafeFilter(
          level, user_key, seqno, kValue, record.value, new_value, skip_until);

        // It would be a problem if it change the value whereas the value_type
        // is still kBlobIndex. For now, just returns kKeep.
        // TODO: we should make rocksdb Filter API support changing value_type
        // assert(decision != CompactionFilter::Decision::kChangeValue);
        if (decision == Decision::kChangeValue) {
          {
            MutexLock l(&db_->mutex_);
            db_->SetBGError(Status::NotSupported(
              "It would be a problem if it change the value whereas the "
              "value_type is still kBlobIndex."));
          }
          decision = Decision::kKeep;
        }
        return decision;
      } else {
        {
          MutexLock l(&db_->mutex_);
          db_->SetBGError(s);
        }
        // GetBlobRecord failed, keep the value.
        return Decision::kKeep;
      }
    }
  }

 private:
  TitanDBImpl *db_;
  const std::string cf_name_;
  std::shared_ptr<BlobStorage> blob_storage_;
  const CompactionFilter *original_filter_;
  const std::unique_ptr<CompactionFilter> owned_filter_;
  bool skip_value_;
  bool enable_ttl_;
  std::string filter_name_;
};

class TitanCompactionFilterFactory final : public CompactionFilterFactory {
 public:
  TitanCompactionFilterFactory(
      const CompactionFilter *original_filter,
      std::shared_ptr<CompactionFilterFactory> original_filter_factory,
      TitanDBImpl *db, bool skip_value, bool enable_ttl, const std::string &cf_name)
      : original_filter_(original_filter),
        original_filter_factory_(original_filter_factory),
        titan_db_impl_(db),
        skip_value_(skip_value),
        enable_ttl_(enable_ttl),
        cf_name_(cf_name) {
    assert(original_filter != nullptr || original_filter_factory != nullptr);
    if (original_filter_ != nullptr) {
      factory_name_ = std::string("TitanCompactionFilterFactory.")
                          .append(original_filter_->Name());
    } else {
      factory_name_ = std::string("TitanCompactionFilterFactory.")
                          .append(original_filter_factory_->Name());
      TITAN_LOG_INFO(titan_db_impl_->db_options_.info_log, "In TitanCompactionFilterFactory, factory_name_ = %s", factory_name_.c_str());
    }
  }

  const char *Name() const override { return factory_name_.c_str(); }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context &context) override {
    TITAN_LOG_INFO(titan_db_impl_->db_options_.info_log, "In CreateCompactionFilter");
    assert(original_filter_ != nullptr || original_filter_factory_ != nullptr);

    std::shared_ptr<BlobStorage> blob_storage;
    {
      MutexLock l(&titan_db_impl_->mutex_);
      blob_storage = titan_db_impl_->blob_file_set_
                         ->GetBlobStorage(context.column_family_id)
                         .lock();
    }
    if (blob_storage == nullptr) {
      assert(false);
      // Shouldn't be here, but ignore compaction filter when we hit error.
      return nullptr;
    }

    const CompactionFilter *original_filter = original_filter_;
    std::unique_ptr<CompactionFilter> original_filter_from_factory;
    TITAN_LOG_INFO(titan_db_impl_->db_options_.info_log, "In CreateCompactionFilter, before original_filter == nullptr1");
    if (original_filter == nullptr) {
      original_filter_from_factory =
          original_filter_factory_->CreateCompactionFilter(context);
      original_filter = original_filter_from_factory.get();
    }
    TITAN_LOG_INFO(titan_db_impl_->db_options_.info_log, "In CreateCompactionFilter, before original_filter == nullptr2");
    if (original_filter == nullptr) {
      return nullptr;
    }

    TITAN_LOG_INFO(titan_db_impl_->db_options_.info_log, "In CreateCompactionFilter, before TitanCompactionFilter");
    return std::unique_ptr<CompactionFilter>(new TitanCompactionFilter(
        titan_db_impl_, cf_name_, original_filter,
        std::move(original_filter_from_factory), blob_storage, skip_value_, enable_ttl_));
  }

 private:
  const CompactionFilter *original_filter_;
  std::shared_ptr<CompactionFilterFactory> original_filter_factory_;
  TitanDBImpl *titan_db_impl_;
  bool skip_value_;
  bool enable_ttl_;
  const std::string cf_name_;
  std::string factory_name_;
};

}  // namespace titandb
}  // namespace rocksdb
