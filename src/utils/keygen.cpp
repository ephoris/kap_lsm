#include "keygen.hpp"

#include <random>

#include "zipf.hpp"

Uniform::Uniform(int max) {
  this->dist = std::uniform_int_distribution<int>(0, max);
}

int Uniform::gen(std::mt19937 &engine) { return this->dist(engine); }

Zipf::Zipf(int max) {
  this->dist = opencog::zipf_distribution<int, double>(max);
}

int Zipf::gen(std::mt19937 &engine) { return this->dist(engine); }
