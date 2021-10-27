/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"
#include "key.hpp"
#include "db.hpp"
#include "lmdb.h"
#include "morton.hpp"

#include <cstdio>
#include <cstring>
#include <cstdint>

#include <iostream>
#include <iomanip>
#include <string>

int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if (0 == commandlineArguments.count("cab")) {
    std::cerr << argv[0] << " lists all entries from the 'all' table of a cabinet (an lmdb-based key/value-database)." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --cab=myStore.cab [--mem=32024]" << std::endl;
    std::cerr << "         --cab: name of the database file" << std::endl;
    std::cerr << "         --mem: upper memory size for database in memory in GB, default: 64,000 (representing 64TB)" << std::endl;
    std::cerr << "         --db:  database to list, default: all" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cab=myStore.cab" << std::endl;
    retCode = 1;
  } else {
    const std::string CABINET{commandlineArguments["cab"]};
    const uint64_t MEM{(commandlineArguments["mem"].size() != 0) ? static_cast<uint64_t>(std::stoi(commandlineArguments["mem"])) : 64UL*1024UL};
    const std::string DB{(commandlineArguments["db"].size() != 0) ? commandlineArguments["db"] : "all"};

    MDB_env *env{nullptr};
    const int numberOfDatabases{100};
    const int64_t SIZE_DB = MEM * 1024UL * 1024UL * 1024UL;

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
    if (!checkErrorCode(mdb_env_set_mapsize(env, SIZE_DB), __LINE__, "mdb_env_set_mapsize")) {
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
      retCode = mdb_dbi_open(txn, DB.c_str(), 0 , &dbi);
      if (MDB_NOTFOUND  == retCode) {
        std::clog << "[" << argv[0] << "]: No database '" << DB << "' found in " << CABINET << "." << std::endl;
      }
      else {
        if (std::string::npos != DB.find("-morton")) {
          mdb_set_compare(txn, dbi, &compareMortonKeys);
          // Multiple values are stored by existing timeStamp in nanoseconds.
          mdb_set_dupsort(txn, dbi, &compareKeys);
        }
        // else if (DB == "all") {
        else {
          mdb_set_compare(txn, dbi, &compareKeys);
        }
        uint64_t numberOfEntries{0};
        MDB_stat stat;
        if (!mdb_stat(txn, dbi, &stat)) {
          numberOfEntries = stat.ms_entries;
        }
        std::clog << "[" << argv[0] << "]: Found " << numberOfEntries << " entries in database '" << DB << "' in " << CABINET << std::endl;

        MDB_cursor *cursor;
        if (!(retCode = mdb_cursor_open(txn, dbi, &cursor))) {
          MDB_val key;
          MDB_val value;
          while ((retCode = mdb_cursor_get(cursor, &key, &value, MDB_NEXT)) == 0) {
            if (std::string::npos != DB.find("-morton")) {
              uint64_t morton = *reinterpret_cast<uint64_t*>(key.mv_data);
              morton = be64toh(morton);
              auto decodedLatLon = convertMortonToLatLon(morton);
              int64_t timeStamp{0};
              if (value.mv_size == sizeof(int64_t)) {
                const char *ptr = static_cast<char*>(value.mv_data);
                std::memcpy(&timeStamp, ptr, value.mv_size);
                timeStamp = be64toh(timeStamp);
                std::cout << morton << "(" << decodedLatLon.first << "," << decodedLatLon.second << "): " << timeStamp << std::endl;
              }
            }
            // else if (DB == "all") {
            else {
              const char *ptr = static_cast<char*>(key.mv_data);
              cabinet::Key storedKey = getKey(ptr, key.mv_size);
              std::cout << storedKey.timeStamp() << ": " << storedKey.dataType() << "/" << storedKey.senderStamp() << std::endl;
            }
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
