#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <CLI/CLI.hpp>
#include <iostream>
#include <string>
#include <utility>

#include "kap_compactor.hpp"
#include "kaplsm/kap_compactor.hpp"
#include "kaplsm/kap_options.hpp"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"
#include "spdlog/common.h"

typedef struct environment {
  std::string db_path;
  kaplsm::KapOptions kap_opt;

  int verbose = 0;
  int parallelism = 1;
  int seed = 0;
  bool early_fill_stop = false;
  uint32_t batch_size = 1'000;
  int key_size = 12;

  std::string key_file;
  std::string extra_key_file;
  bool use_key_file = false;

} environment;

environment parse_args(int argc, char *argv[]) {
  CLI::App app{"Database workload executor"};
  environment env;

  app.add_option("db_path", env.db_path, "Database path")->required();
  app.add_option("--key_file", env.key_file, "Key file")->required();
  app.add_option("--extra_key_file", env.extra_key_file, "Key file")->required();

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

  std::ifstream fid(file_path, std::ios::binary);
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

void run_non_empty_reads(rocksdb::DB *db, std::vector<int> keys) {
  rocksdb::ReadOptions read_opt;
  read_opt.fill_cache = false;
  read_opt.verify_checksums = false;
  read_opt.total_order_seek = false;

  for (auto &key : keys) {
    std::string value;
    auto status = db->Get(read_opt, pad_str_from_int(key, 12), &value);
    if (!status.ok()) {
      spdlog::error("Error reading key: {}", key);
      spdlog::error("{}", status.ToString());
    }
  }
}

void wait_for_all_compactions(rocksdb::DB * db) {
  auto wfc_opts = rocksdb::WaitForCompactOptions();
  wfc_opts.wait_for_purge = true;
  wfc_opts.flush = true;
  db->WaitForCompact(wfc_opts);
  db->Flush(rocksdb::FlushOptions());
  db->WaitForCompact(wfc_opts);
}

void build_db(environment &env) {
  spdlog::info("Building DB: {}", env.db_path);
  kaplsm::KapOptions kap_options;
  rocksdb::Options rocksdb_options = load_options(env);
  auto kcompactor = new kaplsm::KapCompactor(rocksdb_options, kap_options);
  rocksdb_options.listeners.emplace_back(kcompactor);

  // Keys will contain ALL keys presently in the database
  auto keys = load_keys(env.key_file);
  // Extra keys are used exclusively for empty_reads and uniqe writes
  auto extra_keys = load_keys(env.extra_key_file);

  rocksdb::DB *db = nullptr;
  rocksdb::Status status = rocksdb::DB::Open(rocksdb_options, env.db_path, &db);
  if (!status.ok()) {
    spdlog::error("Problems opening DB");
    spdlog::error("{}", status.ToString());
    delete db;
    exit(EXIT_FAILURE);
  }
  rocksdb::WriteOptions write_opt;
  write_opt.sync = false;
  write_opt.low_pri = true;
  write_opt.disableWAL = true;
  write_opt.no_slowdown = false;

  wait_for_all_compactions(db);

  spdlog::info("State of the tree:");
  rocksdb::ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);
  std::vector<std::string> file_names;
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

  db->Close();
}

int main(int argc, char *argv[]) {
  spdlog::info("Building database...");
  environment env = parse_args(argc, argv);

  build_db(env);

  return EXIT_SUCCESS;
}
