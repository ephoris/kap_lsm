#pragma once

#include "kap_options.hpp"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"

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
  virtual CompactionTask* PickCompaction(DB* db,
                                         const std::string& cf_name) = 0;

  // Schedule and run the specified compaction task in background.
  virtual void ScheduleCompaction(CompactionTask* task) = 0;
};

struct CompactionTask {
  CompactionTask(DB* _db, Compactor* _compactor,
                 const std::string& _column_family_name,
                 const std::vector<std::string>& _input_file_names,
                 const int _output_level,
                 const CompactionOptions& _compact_options, bool _retry_on_fail)
      : db(_db),
        compactor(_compactor),
        column_family_name(_column_family_name),
        input_file_names(_input_file_names),
        output_level(_output_level),
        compact_options(_compact_options),
        retry_on_fail(_retry_on_fail) {}
  DB* db;
  Compactor* compactor;
  const std::string& column_family_name;
  std::vector<std::string> input_file_names;
  int output_level;
  rocksdb::CompactionOptions compact_options;
  bool retry_on_fail;
};

class KapCompactor : public Compactor {
 public:
  KapCompactor(const rocksdb::Options rocksdb_options,
                  const KapOptions kap_options)
      : rocksdb_options_(rocksdb_options), kap_options_(kap_options) {
    compact_options_.compression = rocksdb_options_.compression;
    compact_options_.output_file_size_limit =
        rocksdb_options_.target_file_size_base;
  }

  ~KapCompactor() {}

  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override;
  CompactionTask* PickCompaction(DB* db, const std::string& cf_name) override;
  void ScheduleCompaction(CompactionTask* task) override;
  static void CompactFiles(void* arg);

 private:
  rocksdb::Options rocksdb_options_;
  KapOptions kap_options_;
  CompactionOptions compact_options_;
};

}  // namespace kaplsm
