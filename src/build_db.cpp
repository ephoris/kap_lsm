#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <spdlog/spdlog.h>

#include <CLI/CLI.hpp>
#include <string>
#include <utility>

#include "kaplsm/kap_compaction.hpp"
#include "kaplsm/kap_options.hpp"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"

typedef struct environment {
  std::string db_path;
  kaplsm::KapOptions kap_opt;

  int verbose = 0;
  int parallelism = 1;
  int seed = 0;
  bool early_fill_stop = false;

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

  try {
    (app).parse((argc), (argv));
  } catch (const CLI::ParseError &e) {
    exit((app).exit(e));
  }

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

rocksdb::Options load_options(environment &env) {
  // rocksdb::Options opt = *rocksdb::Options().PrepareForBulkLoad();
  rocksdb::Options opt;
  opt.create_if_missing = true;
  opt.error_if_exists = true;
  opt.compaction_style = rocksdb::kCompactionStyleNone;
  opt.compression = rocksdb::kNoCompression;
  // Bulk loading so we manually trigger compactions when need be
  opt.level0_file_num_compaction_trigger = -1;
  opt.IncreaseParallelism(env.parallelism);
  opt.disable_auto_compactions = true;
  opt.num_levels = 20;
  // opt.target_file_size_base = UINT64_MAX;
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
  rocksdb::Options rocksdb_opt = load_options(env);
  auto keys = load_keys(env);

  rocksdb::DB *db = nullptr;
  rocksdb::Status status = rocksdb::DB::Open(rocksdb_opt, env.db_path, &db);
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
    auto kv = create_kv_pair(key, 12, env.kap_opt.entry_size);
    batch.Put(kv.first, kv.second);
    if (batch.Count() > 1'000) {
      spdlog::info("Writing batch {}", batch_num);
      db->Write(write_opt, &batch);
      batch.Clear();
      batch_num++;
    }
  }
  if (batch.Count() > 0) {
    spdlog::info("Writing last batch...", batch_num);
    db->Write(write_opt, &batch);
  }
  db->Flush(rocksdb::FlushOptions());

  if (spdlog::get_level() <= spdlog::level::debug) {
    spdlog::debug("Files per level");
    rocksdb::ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);

    std::vector<std::string> file_names;
    int level_idx = 1;
    for (auto &level : cf_meta.levels) {
      std::string level_str = "";
      for (auto &file : level.files) {
        level_str += file.name + ", ";
      }
      level_str =
          level_str == "" ? "EMPTY" : level_str.substr(0, level_str.size() - 2);
      spdlog::debug("Level {} : {}", level_idx, level_str);
      level_idx++;
    }
  }
}

// void build_db(environment &env) {
//   spdlog::info("Building DB: {}", env.db_path);
//   rocksdb::Options rocksdb_opt;
//   tmpdb::FluidOptions fluid_opt;
//
//   rocksdb_opt.create_if_missing = true;
//   rocksdb_opt.error_if_exists = true;
//   rocksdb_opt.compaction_style = rocksdb::kCompactionStyleNone;
//   rocksdb_opt.compression = rocksdb::kNoCompression;
//   // Bulk loading so we manually trigger compactions when need be
//   rocksdb_opt.level0_file_num_compaction_trigger = -1;
//   rocksdb_opt.IncreaseParallelism(env.parallelism);
//
//   rocksdb_opt.disable_auto_compactions = true;
//   rocksdb_opt.write_buffer_size = env.B;
//   rocksdb_opt.num_levels = env.max_rocksdb_levels;
//   // Prevents rocksdb from limiting file size
//   rocksdb_opt.target_file_size_base = UINT64_MAX;
//
//   fill_fluid_opt(env, fluid_opt);
//   DataGenerator *gen;
//   if (env.use_key_file) {
//     gen = new KeyFileGenerator(env.key_file, 2 * env.N, env.seed, "uniform");
//   } else {
//     gen = new RandomGenerator(env.seed);
//   }
//   FluidLSMBulkLoader *fluid_compactor =
//       new FluidLSMBulkLoader(*gen, fluid_opt, rocksdb_opt,
//       env.early_fill_stop);
//   rocksdb_opt.listeners.emplace_back(fluid_compactor);
//
//   rocksdb::BlockBasedTableOptions table_options;
//   if (env.default_on) {
//     table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
//   } else if (env.L > 0) {
//     table_options.filter_policy.reset(rocksdb::NewMonkeyFilterPolicy(
//         env.bits_per_element, (int)env.T, env.L + 1));
//   } else {
//     table_options.filter_policy.reset(rocksdb::NewMonkeyFilterPolicy(
//         env.bits_per_element, (int)env.T,
//         FluidLSMBulkLoader::estimate_levels(env.N, env.T, env.E, env.B) +
//         1));
//   }
//   table_options.no_block_cache = true;
//   rocksdb_opt.table_factory.reset(
//       rocksdb::NewBlockBasedTableFactory(table_options));
//
//   rocksdb::DB *db = nullptr;
//   rocksdb::Status status = rocksdb::DB::Open(rocksdb_opt, env.db_path, &db);
//   if (!status.ok()) {
//     spdlog::error("Problems opening DB");
//     spdlog::error("{}", status.ToString());
//     delete db;
//     exit(EXIT_FAILURE);
//   }
//
//   if (env.bulk_load_mode == tmpdb::bulk_load_type::LEVELS) {
//     status = fluid_compactor->bulk_load_levels(db, env.L);
//   } else {
//     status = fluid_compactor->bulk_load_entries(db, env.N);
//   }
//
//   if (!status.ok()) {
//     spdlog::error("Problems bulk loading: {}", status.ToString());
//     delete db;
//     exit(EXIT_FAILURE);
//   }
//
//   spdlog::info("Waiting for all compactions to finish before closing");
//   // Wait for all compactions to finish before flushing and closing DB
//   while (fluid_compactor->compactions_left_count > 0);
//
//   if (spdlog::get_level() <= spdlog::level::debug) {
//     spdlog::debug("Files per level");
//     rocksdb::ColumnFamilyMetaData cf_meta;
//     db->GetColumnFamilyMetaData(&cf_meta);
//
//     std::vector<std::string> file_names;
//     int level_idx = 1;
//     for (auto &level : cf_meta.levels) {
//       std::string level_str = "";
//       for (auto &file : level.files) {
//         level_str += file.name + ", ";
//       }
//       level_str =
//           level_str == "" ? "EMPTY" : level_str.substr(0, level_str.size() -
//           2);
//       spdlog::debug("Level {} : {}", level_idx, level_str);
//       level_idx++;
//     }
//   }
//
//   write_existing_keys(env, fluid_compactor);
//   fluid_opt.write_config(env.db_path + "/fluid_config.json");
//
//   db->Close();
//   delete db;
// }

int main(int argc, char *argv[]) {
  spdlog::info("Building database...");
  environment env = parse_args(argc, argv);

  build_db(env);

  return EXIT_SUCCESS;
}
