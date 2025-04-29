#pragma once

#include <atomic>
#include <cstdint>

#include "kap_options.hpp"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"

using ROCKSDB_NAMESPACE::CompactionJobInfo;
using ROCKSDB_NAMESPACE::CompactionOptions;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::EventListener;
using ROCKSDB_NAMESPACE::FlushJobInfo;

namespace kaplsm {

struct CompactionTask;

class Compactor : public EventListener {
 public:
  // Picks and returns a compaction task given the specified DB
  // and column family.  It is the caller's responsibility to
  // destroy the returned CompactionTask.  Returns "nullptr"
  // if it cannot find a proper compaction task.
  virtual CompactionTask* PickCompaction(DB* db, const std::string& cf_name,
                                         size_t level_idx) = 0;

  // Schedule and run the specified compaction task in background.
  virtual void ScheduleCompaction(CompactionTask* task) = 0;

  virtual void DecrementCompactionTaskCount() = 0;
};

struct CompactionTask {
  CompactionTask(DB* _db, Compactor* _compactor,
                 const std::string& _column_family_name,
                 const std::vector<std::string>& _input_file_names,
                 const int _output_level, const int _input_level,
                 const CompactionOptions& _compact_options, bool _retry_on_fail)
      : db(_db),
        compactor(_compactor),
        column_family_name(_column_family_name),
        input_file_names(_input_file_names),
        output_level(_output_level),
        input_level(_input_level),
        compact_options(_compact_options),
        retry_on_fail(_retry_on_fail) {}
  DB* db;
  Compactor* compactor;
  const std::string& column_family_name;
  std::vector<std::string> input_file_names;
  int output_level;
  int input_level;
  rocksdb::CompactionOptions compact_options;
  bool retry_on_fail;
};

class KapCompactor : public Compactor {
 public:
  KapCompactor(const rocksdb::Options rocksdb_options,
               const KapOptions kap_options)
      : rocksdb_options_(rocksdb_options), kap_options_(kap_options) {
    compact_options_.compression = rocksdb_options_.compression;
    compact_options_.output_file_size_limit = UINT64_MAX;
  }

  ~KapCompactor() {}

  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override;
  void OnCompactionCompleted(DB* db, const CompactionJobInfo& info) override;

  CompactionTask* PickCompaction(DB* db, const std::string& cf_name,
                                 size_t level_idx) override;

  void ScheduleCompaction(CompactionTask* task) override;

  std::vector<std::string> CheckIfLevelNeedsCompaction(
      rocksdb::LevelMetaData level);

  int GetCompactionTaskCount() { return compaction_task_count_.load(); }

  void DecrementCompactionTaskCount() override { compaction_task_count_--; }

  void WaitForCompactions() {
    while (compaction_task_count_.load() > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  bool CheckTreeKapacities(DB* db) {
    rocksdb::ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);
    for (size_t level_idx = 0;
         level_idx < static_cast<size_t>(this->rocksdb_options_.num_levels);
         level_idx++) {
      auto level = cf_meta.levels[level_idx];
      int kapacity = 1;
      if (level_idx < this->kap_options_.kapacities.size()) {
        kapacity = this->kap_options_.kapacities[level_idx];
      }
      if (level.files.size() > static_cast<size_t>(kapacity)) {
        return false;
      }
    }
    return true;
  }

  bool ScheduleCompactionsAcrossLevels(DB* db) {
    bool had_to_schedule = false;
    for (size_t level_idx = 0;
         level_idx < static_cast<size_t>(this->rocksdb_options_.num_levels) - 1;
         level_idx++) {
      CompactionTask* task = PickCompaction(db, "", level_idx);
      if (task != nullptr) {
        ScheduleCompaction(task);
        had_to_schedule = true;
      }
    }
    return had_to_schedule;
  }

  static void CompactFiles(void* arg);

 private:
  rocksdb::Options rocksdb_options_;
  KapOptions kap_options_;
  CompactionOptions compact_options_;
  std::atomic<int> compaction_task_count_{0};
};

}  // namespace kaplsm
