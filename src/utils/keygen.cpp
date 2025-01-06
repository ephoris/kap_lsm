#include "keygen.hpp"

#include <random>
#include <string>

#include "rocksdb/slice.h"
#include "zipf.hpp"

Uniform::Uniform(int max) {
  this->dist = std::uniform_int_distribution<int>(0, max);
}

int Uniform::gen(std::mt19937 &engine) { return this->dist(engine); }

Zipf::Zipf(int max) {
  this->dist = opencog::zipf_distribution<int, double>(max);
}

int Zipf::gen(std::mt19937 &engine) { return this->dist(engine); }

KeyGenerator::KeyGenerator(int key_size, int max, int seed) {
  this->key_size = key_size;
  this->max = max;
  this->engine = std::mt19937(seed);
  this->dist = new Uniform(max);
}

std::string KeyGenerator::padded_str_from_int(int num) {
  auto num_str = std::to_string(num);
  auto padded_key =
      std::string(this->key_size - num_str.length(), '0') + num_str;

  return padded_key;
}

rocksdb::Slice KeyGenerator::key_from_int(int num) {
  return rocksdb::Slice(this->padded_str_from_int(num));
}

std::string KeyGenerator::gen_random_key() {
  auto key = this->dist->gen(this->engine);
  return this->padded_str_from_int(key);
}
