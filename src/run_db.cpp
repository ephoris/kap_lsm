#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <CLI/CLI.hpp>
#include <algorithm>
#include <iostream>
#include <random>
#include <string>
#include <utility>

#include "kap_compactor.hpp"
#include "kaplsm/kap_compactor.hpp"
#include "kaplsm/kap_options.hpp"
#include "rocksdb/db.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"

#define PAGESIZE 4096

typedef struct environment {
  std::string db_path;
  kaplsm::KapOptions kap_opt;

  int verbose = 0;
  int parallelism = 1;
  int seed = 0;
  bool early_fill_stop = false;
  uint32_t batch_size = 1'000;
  int key_size = 12;

  int num_writes = 1'000;
  int num_empty_reads = 1'000;
  int num_range_reads = 1'000;
  int num_non_empty_reads = 1'000;

  std::string key_file;
  std::string extra_key_file;
  bool use_key_file = false;

} environment;

environment parse_args(int argc, char *argv[]) {
  CLI::App app{"Database workload executor"};
  environment env;

  app.add_option("db_path", env.db_path, "Database path")->required();
  app.add_option("--key_file", env.key_file, "Key file")->required();
  app.add_option("--extra_key_file", env.extra_key_file, "Key file")
      ->required();
  app.add_option("--num_writes", env.num_writes, "Number of writes");
  app.add_option("--num_empty_reads", env.num_empty_reads,
                 "Number of empty reads");
  app.add_option("--num_range_reads", env.num_range_reads,
                 "Number of range reads");
  app.add_option("--num_non_empty_reads", env.num_non_empty_reads,
                 "Number of non-empty reads");

  // Misc commands
  app.add_option("--parallelism", env.parallelism, "Number of worker threads");
  app.add_option("--seed", env.seed, "Random seed");
  app.add_flag("-v,--verbosity", "verbosity");

  try {
    (app).parse((argc), (argv));
  } catch (const CLI::ParseError &e) {
    exit((app).exit(e));
  }

  switch (app.count("-v")) {
    case 1:
      spdlog::set_level(spdlog::level::debug);
      break;
    case 2:
      spdlog::set_level(spdlog::level::trace);
      break;
    default:
      spdlog::set_level(spdlog::level::info);
  }
  spdlog::info("Verbosity {}", app.count("-v"));

  return env;
}

std::vector<int> load_keys(std::string file_path) {
  int num;
  std::vector<int> vec;
  spdlog::trace("Loading keys from: {}", file_path);

  std::ifstream fid(file_path, std::ios::binary);
  if (!fid.is_open()) {
    spdlog::error("Error opening file: {}", file_path);
    exit(EXIT_FAILURE);
  }

  while (fid.read(reinterpret_cast<char *>(&num), sizeof(int))) {
    vec.push_back(num);
  }
  fid.close();

  return vec;
}

std::string pad_str_from_int(int num, int size) {
  auto num_str = std::to_string(num);
  auto padded_key = std::string(size - num_str.length(), '0') + num_str;

  return padded_key;
}

std::pair<std::string, std::string> create_kv_pair(int key, int key_size,
                                                   int entry_size) {
  auto padded_key = pad_str_from_int(key, key_size);
  auto val_str = std::string(entry_size - padded_key.length(), 'a');

  return std::pair(padded_key, val_str);
}

rocksdb::Options load_options(environment &env) {
  // rocksdb::Options opt = *rocksdb::Options().PrepareForBulkLoad();
  rocksdb::Options opt;
  opt.create_if_missing = false;
  opt.error_if_exists = false;
  opt.compaction_style = rocksdb::kCompactionStyleNone;
  opt.compression = rocksdb::kNoCompression;
  // Bulk loading so we manually trigger compactions when need be
  // Generally we will set threads to 1 to get single thread numbers
  opt.IncreaseParallelism(env.parallelism);
  opt.use_direct_reads = true;
  opt.use_direct_io_for_flush_and_compaction = true;
  opt.advise_random_on_open = false;
  opt.random_access_max_buffer_size = 0;
  opt.avoid_unnecessary_blocking_io = true;
  opt.num_levels = 20;

  // Slow down triggers
  opt.level0_slowdown_writes_trigger = 2 * (env.kap_opt.kapacities[0] + 1);
  opt.level0_stop_writes_trigger = 3 * (env.kap_opt.kapacities[0] + 1);
  opt.level0_file_num_compaction_trigger = env.kap_opt.kapacities[0];

  // Classic LSM parameters
  opt.target_file_size_multiplier = env.kap_opt.size_ratio;
  opt.target_file_size_base = env.kap_opt.buffer_size;
  opt.write_buffer_size = env.kap_opt.buffer_size;

  // Monkey filter policy
  rocksdb::BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(rocksdb::NewMonkeyFilterPolicy(
      env.kap_opt.bits_per_element, env.kap_opt.size_ratio, 20));
  table_options.no_block_cache = true;
  opt.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

  return opt;
}

void wait_for_all_compactions(rocksdb::DB *db) {
  auto wfc_opts = rocksdb::WaitForCompactOptions();
  wfc_opts.wait_for_purge = true;
  wfc_opts.flush = true;
  db->WaitForCompact(wfc_opts);
  db->Flush(rocksdb::FlushOptions());
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

std::chrono::milliseconds read_keys(rocksdb::DB *db, std::vector<int> &keys) {
  rocksdb::ReadOptions read_opt;
  read_opt.fill_cache = false;
  read_opt.verify_checksums = false;
  read_opt.total_order_seek = false;

  auto read_start = std::chrono::high_resolution_clock::now();
  for (auto &key : keys) {
    std::string value;
    spdlog::trace("Reading key: {}", key);
    auto status = db->Get(read_opt, pad_str_from_int(key, 12), &value);
    if (!status.ok() && !status.IsNotFound()) {
      spdlog::error("Error reading key: {}", key);
      spdlog::error("{}", status.ToString());
    }
  }
  auto read_end = std::chrono::high_resolution_clock::now();
  auto read_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      read_end - read_start);

  return read_duration;
}

std::chrono::milliseconds range_reads(environment env, rocksdb::DB *db,
                                      std::vector<int> &exisiting_keys) {
  rocksdb::ReadOptions read_opt;
  read_opt.fill_cache = false;
  read_opt.verify_checksums = false;
  read_opt.total_order_seek = false;
  int key_hop = (PAGESIZE / env.kap_opt.entry_size);
  std::sort(exisiting_keys.begin(), exisiting_keys.end());
  std::vector<int> keys(exisiting_keys.begin(),
                        exisiting_keys.begin() + env.num_range_reads);
  std::uniform_int_distribution<int> dist(0, keys.size() - key_hop);
  std::mt19937 engine(env.seed);

  auto range_read_start = std::chrono::high_resolution_clock::now();
  for (int count = 0; count < env.num_range_reads; count++) {
    int index = dist(engine);
    auto lower_key = pad_str_from_int(exisiting_keys[index], 12);
    auto upper_key = pad_str_from_int(exisiting_keys[index + key_hop], 12);
    read_opt.iterate_upper_bound = new rocksdb::Slice(upper_key);
    spdlog::trace("Range read: {} -> {}", lower_key, upper_key);
    auto it = db->NewIterator(read_opt);
    for (it->Seek(rocksdb::Slice(lower_key)); it->Valid(); it->Next()) {
      auto value = it->value().ToString();
    }
    delete it;
  }
  auto range_read_end = std::chrono::high_resolution_clock::now();
  auto range_read_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(range_read_end -
                                                            range_read_start);

  return range_read_duration;
}

std::pair<std::chrono::milliseconds, std::chrono::milliseconds> write_keys(
    environment &env, rocksdb::DB *db, int num_keys) {
  rocksdb::WriteOptions write_opt;
  write_opt.sync = false;
  write_opt.low_pri = true;
  write_opt.disableWAL = true;
  write_opt.no_slowdown = false;

  spdlog::debug("Keygen for dist: [{}, {}]", num_keys, 2 * num_keys);
  std::uniform_int_distribution<int> dist(num_keys, 2 * num_keys);
  std::mt19937 engine(42);
  spdlog::debug("Example key: {}", dist(engine));
  spdlog::trace("Flushing DB to get into correct state");

  // Force compaction of all files in Level 0
  // Use an empty key range to compact everything
  rocksdb::ColumnFamilyMetaData cf_meta;
  rocksdb::CompactionOptions opt;
  db->GetColumnFamilyMetaData(&cf_meta);
  spdlog::debug("Force compaction of all files in Level 0 to prevent deadlock");
  if (cf_meta.levels[0].files.size() > 0) {
    std::vector<std::string> file_names;
    spdlog::debug("Files in Level 0: {}", cf_meta.levels[0].files.size());
    for (auto &file : cf_meta.levels[0].files) {
      file_names.push_back(file.name);
    }
    db->CompactFiles(opt, file_names, 1);
  }
  wait_for_all_compactions(db);
  spdlog::debug("Finished force compaction, starting writes");
  auto kv = create_kv_pair(dist(engine), 12, env.kap_opt.entry_size);
  spdlog::debug("Example key to write: {}", kv.first.data());

  auto write_start = std::chrono::high_resolution_clock::now();
  for (auto write_idx = 0; write_idx < env.num_writes; write_idx++) {
    // Adding num_keys to ensure all keys are unique writes
    kv = create_kv_pair(dist(engine), 12, env.kap_opt.entry_size);
    auto status = db->Put(write_opt, kv.first, kv.second);
    spdlog::trace("Writing key: {}", kv.first.data());
    if (!status.ok()) {
      spdlog::error("Error writing key: {}", kv.first.data());
      spdlog::error("{}", status.ToString());
    }
  }
  auto write_end = std::chrono::high_resolution_clock::now();
  auto write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      write_end - write_start);

  auto remaining_compactions_start = std::chrono::high_resolution_clock::now();
  wait_for_all_compactions(db);
  auto remaining_compactions_end = std::chrono::high_resolution_clock::now();
  auto remaining_compactions_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          remaining_compactions_end - remaining_compactions_start);

  return std::pair(write_duration, remaining_compactions_duration);
}

void run_workload(environment &env) {
  spdlog::info("Building DB: {}", env.db_path);
  kaplsm::KapOptions kap_options(env.db_path + "/kap_options.json");
  rocksdb::Options rocksdb_options = load_options(env);
  rocksdb_options.statistics = rocksdb::CreateDBStatistics();
  auto kcompactor = new kaplsm::KapCompactor(rocksdb_options, kap_options);
  rocksdb_options.listeners.emplace_back(kcompactor);

  // Keys will contain ALL keys presently in the database
  auto keys = load_keys(env.key_file);
  spdlog::debug("Keys size: {}", keys.size());
  spdlog::debug("keys.at(0) = {}", keys.at(0));
  // Extra keys are used exclusively for empty_reads and uniqe writes
  auto extra_keys = load_keys(env.extra_key_file);
  spdlog::debug("Extra key size: {}", extra_keys.size());
  spdlog::debug("extra_keys.at(0) = {}", extra_keys.at(0));

  rocksdb::DB *db = nullptr;
  rocksdb::Status status = rocksdb::DB::Open(rocksdb_options, env.db_path, &db);
  if (!status.ok()) {
    spdlog::error("Problems opening DB");
    spdlog::error("{}", status.ToString());
    delete db;
    exit(EXIT_FAILURE);
  }

  std::mt19937 gen(env.seed);
  std::shuffle(keys.begin(), keys.end(), gen);
  std::shuffle(extra_keys.begin(), extra_keys.end(), gen);

  rocksdb_options.statistics->Reset();
  spdlog::info("Running Empty Reads");
  std::vector<int> empty_read_keys(extra_keys.begin(),
                                   extra_keys.begin() + env.num_empty_reads);
  spdlog::debug("Empty read keys size: {}", empty_read_keys.size());
  auto empty_read_duration = read_keys(db, empty_read_keys);

  spdlog::info("Running Non-Empty Reads");
  std::vector<int> non_empty_read_keys(keys.begin(),
                                       keys.begin() + env.num_non_empty_reads);
  auto non_empty_read_duration = read_keys(db, non_empty_read_keys);

  spdlog::info("Running Range Reads");
  auto range_read_duration = range_reads(env, db, keys);

  spdlog::info("Running Writes");
  int max_base = *std::max_element(keys.begin(), keys.end());
  int max_extra = *std::max_element(extra_keys.begin(), extra_keys.end());
  int max_key = std::max(max_base, max_extra);
  auto write_duration = write_keys(env, db, max_key);

  log_state_of_tree(db);
  spdlog::info("Empty Reads took {} ms", empty_read_duration.count());
  spdlog::info("Non-Empty Reads took {} ms", non_empty_read_duration.count());
  spdlog::info("Range Reads took {} ms", range_read_duration.count());
  spdlog::info("Writes took {} ms", write_duration.first.count());

  std::map<std::string, uint64_t> stats;
  rocksdb_options.statistics->getTickerMap(&stats);

  spdlog::info("(l0, l1, l2plus) : ({}, {}, {})", stats["rocksdb.l0.hit"],
               stats["rocksdb.l1.hit"], stats["rocksdb.l2andup.hit"]);
  spdlog::info("(bf_true_neg, bf_pos, bf_true_pos) : ({}, {}, {})",
               stats["rocksdb.bloom.filter.useful"],
               stats["rocksdb.bloom.filter.full.positive"],
               stats["rocksdb.bloom.filter.full.true.positive"]);
  spdlog::info(
      "(bytes_written, compact_read, compact_write, flush_write) : ({}, {}, "
      "{}, {})",
      stats["rocksdb.bytes.written"], stats["rocksdb.compact.read.bytes"],
      stats["rocksdb.compact.write.bytes"], stats["rocksdb.flush.write.bytes"]);
  spdlog::info("(block_read_count) : ({})",
               rocksdb::get_perf_context()->block_read_count);
  spdlog::info("(z0, z1, q, w) : ({}, {}, {}, {})", empty_read_duration.count(),
               non_empty_read_duration.count(), range_read_duration.count(),
               write_duration.first.count());
  spdlog::info("(remaining_compactions_duration) : ({})",
               write_duration.second.count());

  db->Close();
}

int main(int argc, char *argv[]) {
  spdlog::info("Building database...");
  environment env = parse_args(argc, argv);

  run_workload(env);

  return EXIT_SUCCESS;
}
