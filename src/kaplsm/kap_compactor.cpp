#include "kap_compactor.hpp"

#include <spdlog/spdlog.h>

#include <cmath>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"

using ROCKSDB_NAMESPACE::ColumnFamilyMetaData;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::FlushJobInfo;

using namespace kaplsm;

// When flush happens, it determines whether to trigger compaction. If
// triggered_writes_stop is true, it will also set the retry flag of
// compaction-task to true.
void KapCompactor::OnFlushCompleted(DB* db, const FlushJobInfo& info) {
  for (auto level_idx = this->rocksdb_options_.num_levels - 1; level_idx >= 0;
       level_idx--) {
    CompactionTask* task =
        PickCompaction(db, info.cf_name, static_cast<size_t>(level_idx));
    if (task != nullptr) {
      if (info.triggered_writes_stop) {
        task->retry_on_fail = true;
      }
      ScheduleCompaction(task);
    }
  }
}

// When a compaction finishes, we will also check to make sure the state of the
// tree is OK. This SHOULD be called until the tree returns no more viable
// compaction jobs
void KapCompactor::OnCompactionCompleted(DB* db,
                                         const CompactionJobInfo& info) {
  for (size_t level_idx = 0;
       level_idx < static_cast<size_t>(this->rocksdb_options_.num_levels) - 1;
       level_idx++) {
    CompactionTask* task = PickCompaction(db, info.cf_name, level_idx);
    if (task != nullptr) {
      ScheduleCompaction(task);
    }
  }
}

std::vector<std::string> KapCompactor::CheckIfLevelNeedsCompaction(
    rocksdb::LevelMetaData level) {
  int level_kapacity = 1;
  if (static_cast<size_t>(level.level) < this->kap_options_.kapacities.size()) {
    level_kapacity = this->kap_options_.kapacities[level.level];
  }
  if (level.files.size() <= static_cast<size_t>(level_kapacity)) {
    return {};
  }

  std::vector<std::string> input_file_names;
  for (auto file : level.files) {
    if (!file.being_compacted) {
      input_file_names.push_back(file.name);
    }
  }

  return input_file_names;
}

// PickCompaction looks at one paritcular level and checks whether or not the
// level is full and needs to compact. If no compaction is needed, returns a
// nullptr
CompactionTask* KapCompactor::PickCompaction(DB* db, const std::string& cf_name,
                                             size_t level_idx) {
  ColumnFamilyMetaData cf_meta;
  rocksdb::CompactionOptions opt;
  db->GetColumnFamilyMetaData(&cf_meta);
  auto level = cf_meta.levels[level_idx];
  auto size_ratio = this->rocksdb_options_.target_file_size_multiplier;
  auto file_base = this->rocksdb_options_.target_file_size_base;
  int k_level = 1;
  if (static_cast<size_t>(level.level) < this->kap_options_.kapacities.size()) {
    k_level = this->kap_options_.kapacities.at(level.level);
  }
  // Each level is (total_level_size) / (num_file_kapacity) where
  // total_level_size is equal to m*T^l where l is level, T is size ratio, and m
  // is the size of the memory buffer. We add +1 since RocksDB starts numbering
  // levels at 0.
  auto file_size = (file_base * pow(size_ratio, level.level + 1)) / k_level;
  // Adding an extra ~4% bytes to accomedate for file meta data
  opt.output_file_size_limit = 1.04 * file_size;
  auto input_file_names = this->CheckIfLevelNeedsCompaction(level);
  if (input_file_names.size() < 1) {
    return nullptr;
  }

  return new CompactionTask(db, this, cf_name, input_file_names,
                            level.level + 1, level.level, opt, false);
}

// Schedule the specified compaction task in background.
void KapCompactor::ScheduleCompaction(CompactionTask* task) {
  spdlog::trace("Scheduling compaction {} -> {}", task->input_level,
                task->output_level);
  this->compaction_task_count_++;
  rocksdb_options_.env->Schedule(&KapCompactor::CompactFiles, task);
}

void KapCompactor::CompactFiles(void* arg) {
  std::unique_ptr<CompactionTask> task(static_cast<CompactionTask*>(arg));
  assert(task);
  assert(task->db);
  rocksdb::Status s = task->db->CompactFiles(
      task->compact_options, task->input_file_names, task->output_level);
  spdlog::trace("CompactFiles() finished with status {}", s.ToString());
  task->compactor->DecrementCompactionTaskCount();
  if (!s.ok() && !s.IsIOError() && task->retry_on_fail) {
    // If a compaction task with its retry_on_fail=true failed,
    // try to schedule another compaction in case the reason
    // is not an IO error.
    CompactionTask* new_task = task->compactor->PickCompaction(
        task->db, task->column_family_name, task->input_level);
    task->compactor->ScheduleCompaction(new_task);
  }
}
