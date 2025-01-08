#pragma once

#include <spdlog/spdlog.h>

#include <cstdint>
#include <fstream>
#include <limits>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

namespace kaplsm {

class KapOptions {
 public:
  int size_ratio = 2;
  std::vector<int> kapacities;
  int buffer_size = 1 << 20;  //> bytes (B) defaults 1 MB
  int entry_size = 512;       //> bytes (E)
  // bits per element per bloom filter at all levels (h)
  double bits_per_element = 5.0;
  uint64_t fixed_file_size = std::numeric_limits<uint64_t>::max();
  unsigned long num_keys = 0;
  unsigned int levels = 0;

  KapOptions() : kapacities(20, 1) {};
  KapOptions(std::string config_path) { ReadConfig(config_path); }

  bool ReadConfig(std::string config_path) {
    nlohmann::json cfg;
    std::ifstream read_cfg(config_path);
    if (!read_cfg.is_open()) {
      spdlog::error("Unable to read config: {}", config_path);
      return false;
    }
    read_cfg >> cfg;

    this->size_ratio = cfg["size_ratio"];
    this->kapacities = cfg["kapacities"].get<std::vector<int>>();
    this->buffer_size = cfg["buffer_size"];
    this->entry_size = cfg["entry_size"];
    this->bits_per_element = cfg["bits_per_element"];
    this->fixed_file_size = cfg["fixed_file_size"];
    this->num_keys = cfg["num_keys"];
    this->levels = cfg["levels"];

    return true;
  }

  bool WriteConfig(std::string config_path) {
    nlohmann::json cfg;
    cfg["size_ratio"] = this->size_ratio;
    cfg["kapacities"] = this->kapacities;
    cfg["buffer_size"] = this->buffer_size;
    cfg["entry_size"] = this->entry_size;
    cfg["bits_per_element"] = this->bits_per_element;
    cfg["fixed_file_size"] = this->fixed_file_size;
    cfg["num_keys"] = this->num_keys;
    cfg["levels"] = this->levels;

    std::ofstream out_cfg(config_path);
    if (!out_cfg.is_open()) {
      spdlog::error("Unable to create or open file: {}", config_path);
      return false;
    }
    out_cfg << cfg.dump(4) << std::endl;
    out_cfg.close();
    spdlog::info("Writing configuration file at {}", config_path);

    return true;
  }
};

}  // namespace kaplsm
