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

#include <fstream>
#include <iostream>
#include <iomanip>
#include <map>
#include <string>

typedef unsigned __int128 uint128_t;

int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if (0 == commandlineArguments.count("cabinet")) {
    std::cerr << argv[0] << " exports all entries from the 'all' table of a cabinet (an lmdb-based key/value-database) as Envelopes to a .rec-file." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --cabinet=myStore.cabinet" << std::endl;
    std::cerr << "         --cabinet: name of the database file" << std::endl;
    std::cerr << "         --rec:     name of the rec file (optional; otherwise, a new file based on the .cabinet file with .rec as suffix is created)" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cabinet=myStore.cabinet --rec=myRecFile.rec" << std::endl;
    retCode = 1;
  } else {
    const std::string CABINET{commandlineArguments["cabinet"]};
    const std::string REC{(commandlineArguments["rec"].size() != 0) ? commandlineArguments["rec"] : "./" + CABINET + ".rec"};

    MDB_env *env{nullptr};
    const int numberOfDatabases{100};
    const int64_t SIZE_DB = 64UL * 1024UL * 1024UL * 1024UL * 1024UL;

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

    std::fstream recFile;
    recFile.open(REC.c_str(), std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
    if (recFile.good()) {
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
          uint64_t entries{0};
          int32_t oldPercentage{-1};
          MDB_val key;
          MDB_val val;

          while ((retCode = mdb_cursor_get(cursor, &key, &val, MDB_NEXT_NODUP)) == 0) {
            char *ptr = static_cast<char*>(key.mv_data);
            // b0-b7: int64_t for timeStamp in nanoseconds
            uint16_t offset{sizeof(int64_t) /*field 1: timeStamp in nanoseconds*/};
            // b8-b11: int32_t for dataType
            offset += sizeof(int32_t);
            // b12-b15: uint32_t for senderStamp
            offset += sizeof(uint32_t);

            // b16: uint8_t: version
            uint8_t version{0};
            offset += sizeof(uint8_t);
            if (0 == version) {
              // b17-b18: uint16_t: length of the in-key value
              const uint16_t length = *(reinterpret_cast<uint16_t*>(ptr + offset));
              offset += sizeof(uint16_t);
              if (0 < length) {
                // value if contained in the key
                recFile.write(ptr + offset, length);
              }
              else {
                // value is not stored in the key
                recFile.write(static_cast<char*>(val.mv_data), val.mv_size);
              }
              entries++;
            }
   
            const int32_t percentage = static_cast<int32_t>(static_cast<float>(entries * 100.0f) / static_cast<float>(numberOfEntries));
            if ((percentage % 5 == 0) && (percentage != oldPercentage)) {
              std::clog << "[" << argv[0] << "]: Processed " << percentage << "% (" << entries << " entries) from " << CABINET << "." << std::endl;
              oldPercentage = percentage;
              recFile.flush();
            }
          }
          recFile.close();
          mdb_cursor_close(cursor);
        }
      }
      mdb_txn_abort(txn);
      if (dbi) {
        mdb_dbi_close(env, dbi);
      }
    }
    else {
      std::cerr << "[" << argv[0] << "]: Error opening " << REC << std::endl; 
    }
    if (env) {
      mdb_env_close(env);
    }
  }
  return retCode;
}
