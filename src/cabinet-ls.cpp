/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"
#include "lmdb.h"

#include <cstdio>
#include <cstring>
#include <cstdint>

#include <iostream>
#include <iomanip>
#include <map>
#include <string>

typedef unsigned __int128 uint128_t;

int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if (0 == commandlineArguments.count("cabinet")) {
    std::cerr << argv[0] << " lists all entries from the 'all' table of a cabinet (an lmdb-based key/value-database)." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --cabinet=myStore.cabinet" << std::endl;
    std::cerr << "         --cabinet: name of the database file" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cabinet=myStore.cabinet" << std::endl;
    retCode = 1;
  } else {
    const std::string CABINET{commandlineArguments["cabinet"]};

    MDB_env *env{nullptr};
    const int numberOfDatabases{100};
    const int64_t SIZE_120TB = 120UL * 1024UL * 1024UL * 1024UL * 1024UL;

    // lambda to check the interaction with the database.
    auto checkErrorCode = [_argv=argv](int32_t rc, int32_t line, std::string caller) {
      if (0 != rc) {
        std::cerr << "[" << _argv[0] << "]: " << caller << ", line " << line << ": (" << rc << ") " << mdb_strerror(rc) << std::endl; 
      }
      return (0 == rc);
    };

    if (!checkErrorCode(mdb_env_create(&env), __LINE__, "mdb_env_create")) {
      return 1;
    }
    if (!checkErrorCode(mdb_env_set_maxdbs(env, numberOfDatabases), __LINE__, "mdb_env_set_maxdbs")) {
      mdb_env_close(env);
      return 1;
    }
    if (!checkErrorCode(mdb_env_set_mapsize(env, SIZE_120TB), __LINE__, "mdb_env_set_mapsize")) {
      mdb_env_close(env);
      return 1;
    }
    if (!checkErrorCode(mdb_env_open(env, CABINET.c_str(), MDB_NOSUBDIR|MDB_RDONLY, 0600), __LINE__, "mdb_env_open")) {
      mdb_env_close(env);
      return 1;
    }

    {
      MDB_txn *txn{nullptr};
      MDB_dbi dbi{0};
      if (!checkErrorCode(mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn), __LINE__, "mdb_txn_begin")) {
        mdb_env_close(env);
        return (retCode = 1);
      }
      retCode = mdb_dbi_open(txn, "all", 0 , &dbi);
      if (MDB_NOTFOUND  == retCode) {
        std::clog << "[" << argv[0] << "]: No database 'all' found in " << CABINET << "." << std::endl;
      }
      else {
        uint64_t numberOfEntries{0};
        MDB_stat stat;
        if (!mdb_stat(txn, dbi, &stat)) {
          numberOfEntries = stat.ms_entries;
        }
        std::clog << "[" << argv[0] << "]: Found " << numberOfEntries << " entries in database 'all' in " << CABINET << std::endl;

        MDB_cursor *cursor;
        if (!(retCode = mdb_cursor_open(txn, dbi, &cursor))) {
          MDB_val key;
          while ((retCode = mdb_cursor_get(cursor, &key, nullptr, MDB_NEXT_NODUP)) == 0) {
            __int128 tmp = *(static_cast<__int128*>(key.mv_data));
            const int64_t timeStamp = static_cast<int64_t>((tmp>>64));
            const int64_t dataType = static_cast<int64_t>((tmp&0xFFFFFFFF00000000)) >> 32;
            const int64_t senderStamp = static_cast<int64_t>((tmp&0xFFFFFFFF));
            std::clog << timeStamp << ": " << dataType << "/" << senderStamp << std::endl;
          }
          mdb_cursor_close(cursor);
        }
      }
      mdb_txn_abort(txn);
      if (dbi) {
        mdb_dbi_close(env, dbi);
      }
    }
    if (env) {
      mdb_env_close(env);
    }
  }
  return retCode;
}
