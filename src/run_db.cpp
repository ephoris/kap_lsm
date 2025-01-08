#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <CLI/CLI.hpp>
#include <iostream>
#include <random>
#include <string>
#include <utility>

#include "kap_compactor.hpp"
#include "kaplsm/kap_compactor.hpp"
#include "kaplsm/kap_options.hpp"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
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

std::pair<rocksdb::Slice, rocksdb::Slice> create_kv_pair(int key, int key_size,
                                                         int entry_size) {
  auto padded_key = pad_str_from_int(key, key_size);
  rocksdb::Slice val = std::string(entry_size - padded_key.length(), 'a');

  return std::pair<rocksdb::Slice, rocksdb::Slice>(padded_key, val);
}

rocksdb::Options load_options(environment &env) {
  // rocksdb::Options opt = *rocksdb::Options().PrepareForBulkLoad();
  rocksdb::Options opt;
  opt.create_if_missing = false;
  opt.error_if_exists = false;
  opt.compaction_style = rocksdb::kCompactionStyleNone;
  opt.compression = rocksdb::kNoCompression;
  // Bulk loading so we manually trigger compactions when need be
  opt.IncreaseParallelism(env.parallelism);
  opt.use_direct_reads = true;
  opt.use_direct_io_for_flush_and_compaction = true;
  opt.advise_random_on_open = false;
  opt.random_access_max_buffer_size = 0;
  opt.avoid_unnecessary_blocking_io = true;
  opt.num_levels = 20;

  // Slow down triggers
  opt.level0_slowdown_writes_trigger = 2 * (env.kap_opt.size_ratio + 1);
  opt.level0_stop_writes_trigger = 3 * (env.kap_opt.size_ratio + 1);
  opt.level0_file_num_compaction_trigger = env.kap_opt.size_ratio;

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

  auto range_read_start = std::chrono::high_resolution_clock::now();
  for (size_t range_count = 0; range_count < keys.size(); range_count++) {
    auto lower_key = pad_str_from_int(exisiting_keys[range_count], 12);
    auto upper_key =
        pad_str_from_int(exisiting_keys[range_count + key_hop], 12);
    read_opt.iterate_upper_bound = new rocksdb::Slice(upper_key);

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

std::chrono::milliseconds write_keys(rocksdb::DB *db, std::vector<int> &keys) {
  rocksdb::WriteOptions write_opt;
  write_opt.sync = false;
  write_opt.low_pri = true;
  write_opt.disableWAL = true;
  write_opt.no_slowdown = false;

  auto write_start = std::chrono::high_resolution_clock::now();
  for (auto &key : keys) {
    auto kv = create_kv_pair(key, 12, 100);
    auto status = db->Put(write_opt, kv.first, kv.second);
    if (!status.ok()) {
      spdlog::error("Error writing key: {}", key);
      spdlog::error("{}", status.ToString());
    }
  }
  wait_for_all_compactions(db);
  auto write_end = std::chrono::high_resolution_clock::now();
  auto write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      write_end - write_start);

  return write_duration;
}

void run_workload(environment &env) {
  spdlog::info("Building DB: {}", env.db_path);
  kaplsm::KapOptions kap_options(env.db_path + "/kap_options.json");
  rocksdb::Options rocksdb_options = load_options(env);
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
  std::vector<int> keys_to_write(extra_keys.begin(),
                                 extra_keys.begin() + env.num_writes);
  auto write_duration = write_keys(db, keys_to_write);

  log_state_of_tree(db);
  spdlog::info("Empty Reads took {} ms", empty_read_duration.count());
  spdlog::info("Non-Empty Reads took {} ms", non_empty_read_duration.count());
  spdlog::info("Range Reads took {} ms", range_read_duration.count());
  spdlog::info("Writes took {} ms", write_duration.count());

  db->Close();
}

int main(int argc, char *argv[]) {
  spdlog::info("Building database...");
  environment env = parse_args(argc, argv);

  run_workload(env);

  return EXIT_SUCCESS;
}
