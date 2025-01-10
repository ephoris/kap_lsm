#pragma once

#include "rocksdb/db.h"

void wait_for_all_compactions(rocksdb::DB *db);

void log_state_of_tree(rocksdb::DB *db);
