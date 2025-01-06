#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace kaplsm {

class KapOptions {
 public:
  int size_ratio = 2;
  std::vector<int> kapacities = {1};
  int buffer_size = 1 << 20;  //> bytes (B) defaults 1 MB
  int entry_size = 512;       //> bytes (E)

  // bits per element per bloom filter at all levels (h)
  double bits_per_element = 5.0;

  // bulk_load_type bulk_load_opt = ENTRIES;
  // file_size_policy file_size_policy_opt = INCREASING;
  uint64_t fixed_file_size = std::numeric_limits<uint64_t>::max();

  unsigned long num_keys = 0;
  unsigned int levels = 0;
  size_t file_size = std::numeric_limits<size_t>::max();

  KapOptions() {};
  KapOptions(std::string config_path);
};

}  // namespace kaplsm
