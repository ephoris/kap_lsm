#include "kap_compactor.hpp"

#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"

using ROCKSDB_NAMESPACE::ColumnFamilyMetaData;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::FlushJobInfo;

using namespace kaplsm;

// When flush happens, it determines whether to trigger compaction. If
// triggered_writes_stop is true, it will also set the retry flag of
// compaction-task to true.
void KapCompactor::OnFlushCompleted(DB* db, const FlushJobInfo& info) {
  CompactionTask* task = PickCompaction(db, info.cf_name);
  if (task != nullptr) {
    if (info.triggered_writes_stop) {
      task->retry_on_fail = true;
    }
    // Schedule compaction in a different thread.
    ScheduleCompaction(task);
  }
}

// Always pick a compaction which includes all files whenever possible.
CompactionTask* KapCompactor::PickCompaction(DB* db,
                                                const std::string& cf_name) {
  ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);

  std::vector<std::string> input_file_names;
  for (auto level : cf_meta.levels) {
    for (auto file : level.files) {
      if (file.being_compacted) {
        return nullptr;
      }
      input_file_names.push_back(file.name);
    }
  }
  return new CompactionTask(db, this, cf_name, input_file_names,
                            rocksdb_options_.num_levels - 1, compact_options_,
                            false);
}

// Schedule the specified compaction task in background.
void KapCompactor::ScheduleCompaction(CompactionTask* task) {
  rocksdb_options_.env->Schedule(&KapCompactor::CompactFiles, task);
}

void KapCompactor::CompactFiles(void* arg) {
  std::unique_ptr<CompactionTask> task(static_cast<CompactionTask*>(arg));
  assert(task);
  assert(task->db);
  rocksdb::Status s = task->db->CompactFiles(
      task->compact_options, task->input_file_names, task->output_level);
  printf("CompactFiles() finished with status %s\n", s.ToString().c_str());
  if (!s.ok() && !s.IsIOError() && task->retry_on_fail) {
    // If a compaction task with its retry_on_fail=true failed,
    // try to schedule another compaction in case the reason
    // is not an IO error.
    CompactionTask* new_task =
        task->compactor->PickCompaction(task->db, task->column_family_name);
    task->compactor->ScheduleCompaction(new_task);
  }
}
