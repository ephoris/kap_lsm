#include "utils.hpp"

#include <rocksdb/db.h>
#include <spdlog/spdlog.h>

bool compactions_in_progress(rocksdb::DB *db) {
  uint64_t value = 0;
  db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes", &value);
  spdlog::debug("Estimated compaction bytes {}", value);
  db->GetIntProperty("rocksdb.compaction-pending", &value);
  spdlog::debug("Remaining compactions {}", value);

  return value > 0;
}

void wait_for_all_background_compactions(rocksdb::DB *db) {
  while (compactions_in_progress(db)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void wait_for_all_compactions_and_close_db(rocksdb::DB *db) {
  auto wfc_opts = rocksdb::WaitForCompactOptions();
  wfc_opts.wait_for_purge = true;
  wfc_opts.flush = true;
  wfc_opts.close_db = true;
  db->WaitForCompact(wfc_opts);
}

void log_state_of_tree(rocksdb::DB *db) {
  spdlog::info("State of the tree:");
  rocksdb::ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);
  for (auto &level : cf_meta.levels) {
    std::string level_str = "";
    for (auto &file : level.files) {
      level_str += file.name + ", ";
    }
    level_str =
        level_str == "" ? "EMPTY" : level_str.substr(0, level_str.size() - 2);
    spdlog::info("Level {} | Size: {} | Files: {}", level.level, level.size,
                 level_str);
  }
}
