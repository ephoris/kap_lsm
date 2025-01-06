#pragma once

#include <spdlog/spdlog.h>

#include <cassert>
#include <ctime>
#include <random>

#include "rocksdb/slice.h"
#include "zipf.hpp"

#define KEY_DOMAIN 1000000000

class Distribution {
 public:
  virtual ~Distribution() {}
  virtual int gen(std::mt19937& engine) = 0;
};

class Uniform : public Distribution {
 public:
  Uniform(int max);
  ~Uniform() {}
  int gen(std::mt19937& engine);

 private:
  std::uniform_int_distribution<int> dist;
};

class Zipf : public Distribution {
 public:
  Zipf(int max);
  ~Zipf() {}
  int gen(std::mt19937& engine);

 private:
  opencog::zipf_distribution<int, double> dist;
};

class KeyGenerator {
 public:
  int max;
  int key_size;
  std::mt19937 engine;
  Uniform* dist;

  KeyGenerator(int key_size, int max, int seed = 0);
  ~KeyGenerator() {}

  std::string gen_random_key();
  std::string padded_str_from_int(int num);
  rocksdb::Slice key_from_int(int num);
};
