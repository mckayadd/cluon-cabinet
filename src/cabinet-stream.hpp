/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef CABINET_STREAM_HPP
#define CABINET_STREAM_HPP

#include "cluon-complete.hpp"
#include "db.hpp"
#include "key.hpp"
#include "lmdb.h"

#include <cstdio>
#include <cstring>
#include <cstdint>

#include <fstream>
#include <iostream>
#include <iomanip>
#include <string>

/**
 * This function streams the content of the given cabinet file.
 * It can be used for various use cases, for example to stream the content over network,
 * or to "replay" it into an OD4Session.
 *
 * @param ARGV0 Our name.
 * @param CABINET The file to replay.
 * @param VERBOSE
 * @return 0 on success, 1 otherwise
 */
inline int cabinet_stream(const std::string &ARGV0, const std::string &CABINET, const bool &VERBOSE) {
  int32_t retCode{0};
  MDB_env *env{nullptr};
  const int numberOfDatabases{100};
  const int64_t SIZE_DB = 64UL * 1024UL * 1024UL * 1024UL * 1024UL;

  // lambda to check the interaction with the database.
  auto checkErrorCode = [argv0=ARGV0, VERBOSE](int32_t rc, int32_t line, std::string caller) {
    if ((0 != rc) && VERBOSE) {
      std::cerr << "[" << argv0 << "]: " << caller << ", line " << line << ": (" << rc << ") " << mdb_strerror(rc) << std::endl; 
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

  MDB_txn *txn{nullptr};
  MDB_dbi dbi{0};
  if (!checkErrorCode(mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn), __LINE__, "mdb_txn_begin")) {
    mdb_env_close(env);
    return (retCode = 1);
  }
  retCode = mdb_dbi_open(txn, "all", 0 , &dbi);
  if ((MDB_NOTFOUND  == retCode) && VERBOSE) {
    std::cerr << "[" << ARGV0 << "]: No database 'all' found in " << CABINET << "." << std::endl;
  }
  else {
    uint64_t numberOfEntries{0};
    MDB_stat stat;
    if (!mdb_stat(txn, dbi, &stat)) {
      numberOfEntries = stat.ms_entries;
    }
    if (VERBOSE) {
      std::cerr << "[" << ARGV0 << "]: Found " << numberOfEntries << " entries in database 'all' in " << CABINET << std::endl;
    }

    std::cout.setf(std::ios::binary);
    MDB_cursor *cursor;
    if (!(retCode = mdb_cursor_open(txn, dbi, &cursor))) {
      uint64_t entries{0};
      int32_t oldPercentage{-1};
      MDB_val key;
      MDB_val val;

      while ((retCode = mdb_cursor_get(cursor, &key, &val, MDB_NEXT_NODUP)) == 0) {
        if (VERBOSE) {
          const char *ptr = static_cast<char*>(key.mv_data);
          cabinet::Key storedKey = getKey(ptr, key.mv_size);
          std::cerr << storedKey.timeStamp() << ": " << storedKey.dataType() << "/" << storedKey.senderStamp() << std::endl;
        }

        //recFile.write(static_cast<char*>(val.mv_data), val.mv_size);
        std::cout.write(static_cast<char*>(val.mv_data), val.mv_size);
        std::cout.flush();
        entries++;

        const int32_t percentage = static_cast<int32_t>(static_cast<float>(entries * 100.0f) / static_cast<float>(numberOfEntries));
        if (((percentage % 5 == 0) && (percentage != oldPercentage)) && VERBOSE) {
          std::cerr << "[" << ARGV0 << "]: Processed " << percentage << "% (" << entries << " entries) from " << CABINET << "." << std::endl;
          oldPercentage = percentage;
        }
      }
      mdb_cursor_close(cursor);
    }
  }
  mdb_txn_abort(txn);
  if (dbi) {
    mdb_dbi_close(env, dbi);
  }
  retCode = 0;

  if (env) {
    mdb_env_close(env);
  }
  return retCode;
}

#endif
