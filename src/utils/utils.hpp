#pragma once

#include "rocksdb/db.h"

void wait_for_all_compactions_and_close_db(rocksdb::DB *db);

void log_state_of_tree(rocksdb::DB *db);
