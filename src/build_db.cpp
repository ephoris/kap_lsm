#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <CLI/CLI.hpp>
#include <iostream>
#include <string>
#include <thread>
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
  bool use_key_file = false;

} environment;

environment parse_args(int argc, char *argv[]) {
  CLI::App app{"Database builder"};
  environment env;

  app.add_option("db_path", env.db_path, "Database path")->required();
  app.add_option("--key_file", env.key_file, "Key file")->required();

  // Database parameters
  app.add_option("-T,--size_ratio", env.kap_opt.size_ratio, "Size ratio");
  app.add_option("-K,--kapacities", env.kap_opt.kapacities, "Kapacities list");
  app.add_option("-M,--buffer_size", env.kap_opt.buffer_size, "Buffer size");
  app.add_option("-E,--entry_size", env.kap_opt.entry_size, "Entry size");
  app.add_option("-B,--bits_per_element", env.kap_opt.bits_per_element,
                 "Bloom filter bits");
  app.add_option("-N,--num_keys", env.kap_opt.num_keys, "Number of keys");

  // Misc commands
  app.add_option("--parallelism", env.parallelism, "Number of worker threads");
  app.add_option("--seed", env.seed, "Random seed");
  app.add_option("--batch_size", env.batch_size, "Batch size per write");
  app.add_option("--key_size", env.key_size, "Key size")->default_val(12);
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

std::vector<int> load_keys(environment &env) {
  int num;
  std::vector<int> vec;

  std::ifstream fid(env.key_file, std::ios::binary);
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

bool compactions_in_progress(rocksdb::DB *db) {
  uint64_t compacts_in_progress = 0;
  db->GetIntProperty("rocksdb.background_compactions", &compacts_in_progress);
  spdlog::debug("Remaining compactions {}", compacts_in_progress);

  return compacts_in_progress > 0;
}

void wait_for_all_background_compactions(rocksdb::DB *db) {
  while (compactions_in_progress(db)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

rocksdb::Options load_options(environment &env) {
  // rocksdb::Options opt = *rocksdb::Options().PrepareForBulkLoad();
  rocksdb::Options opt;
  opt.create_if_missing = true;
  opt.error_if_exists = true;
  opt.compaction_style = rocksdb::kCompactionStyleNone;
  opt.compression = rocksdb::kNoCompression;
  // Bulk loading so we manually trigger compactions when need be
  opt.level0_file_num_compaction_trigger = env.kap_opt.size_ratio;
  opt.level0_slowdown_writes_trigger = 20;
  opt.IncreaseParallelism(env.parallelism);
  // opt.disable_auto_compactions = true;
  opt.num_levels = 20;
  // opt.target_file_size_base = UINT64_MAX;
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

void build_db(environment &env) {
  spdlog::info("Building DB: {}", env.db_path);
  kaplsm::KapOptions kap_options;
  rocksdb::Options rocksdb_options = load_options(env);
  auto kcompactor = new kaplsm::KapCompactor(rocksdb_options, kap_options);
  rocksdb_options.listeners.emplace_back(kcompactor);
  auto keys = load_keys(env);

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

  rocksdb::WriteBatch batch;
  int batch_num = 0;

  for (auto key : keys) {
    auto kv = create_kv_pair(key, env.key_size, env.kap_opt.entry_size);
    batch.Put(kv.first, kv.second);
    if (batch.Count() > env.batch_size) {
      spdlog::debug("Writing batch {}", batch_num);
      db->Write(write_opt, &batch);
      batch.Clear();
      batch_num++;
    }
  }
  if (batch.Count() > 0) {
    spdlog::info("Writing last batch...", batch_num);
    db->Write(write_opt, &batch);
  }
  // wait_for_all_background_compactions(db);
  // db->Flush(rocksdb::FlushOptions());
  // wait_for_all_background_compactions(db);
  auto wfc_opts = rocksdb::WaitForCompactOptions();
  wfc_opts.wait_for_purge = true;
  wfc_opts.flush = true;
  db->WaitForCompact(wfc_opts);
  db->Flush(rocksdb::FlushOptions());
  db->WaitForCompact(wfc_opts);

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
