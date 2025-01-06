#include <CLI/CLI.hpp>
#include <random>

#include "spdlog/spdlog.h"

#define MIN_CHUNK_SIZE 1024 * 1024

typedef struct environment {
  std::string key_file;

  int verbose = 0;
  int seed = 0;
  int num_keys = 1'000'000;
  int start = 0;
  int end = 1'000'000;
} environment;

environment parse_args(int argc, char *argv[]) {
  environment env;

  CLI::App app{"Database builder"};

  app.add_option("key_file", env.key_file, "Name of keyfile");
  app.add_option("--num_keys", env.num_keys, "Number of keys");
  app.add_option("--start", env.start, "Start key range");
  app.add_option("--end", env.end, "End key range");
  app.add_option("--seed", env.seed, "Random seed");

  try {
    (app).parse((argc), (argv));
  } catch (const CLI::ParseError &e) {
    exit((app).exit(e));
  }

  return env;
}

int main(int argc, char *argv[]) {
  environment env = parse_args(argc, argv);

  spdlog::info("Generating {} keys", env.num_keys);
  std::random_device rd;
  std::mt19937 gen(rd());

  std::vector<int> vec(env.num_keys);
  std::iota(vec.begin(), vec.end(), 0);
  std::shuffle(vec.begin(), vec.end(), gen);

  spdlog::info("Writing keys to {}", env.key_file);
  std::ofstream fid(env.key_file, std::ios::binary);

  if (!fid.is_open()) {
    std::cerr << "Error opening file!" << std::endl;
    return EXIT_FAILURE;
  }
  fid.write(reinterpret_cast<char *>(vec.data()), env.num_keys * sizeof(int));
  fid.close();

  return EXIT_SUCCESS;
}