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
#include "lz4.h"
#include "lz4hc.h"

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
 * @param 
 * @return 0 on success, 1 otherwise
 */
inline int cabinet_stream(const std::string &ARGV0, const uint64_t &MEM, const std::string &CABINET, const bool &VERBOSE, const std::map<std::string, bool> &mapOfEnvelopesToExport, const int32_t &START, const int32_t &END) {
  int32_t retCode{0};
  uint64_t entries{0};
  MDB_env *env{nullptr};
  const int numberOfDatabases{100};
  const int64_t SIZE_DB = MEM * 1024UL * 1024UL * 1024UL;
  const int64_t START_IN_NS = START * 1000UL * 1000UL * 1000UL;
  const int64_t END_IN_NS = END * 1000UL * 1000UL * 1000UL;

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
    mdb_set_compare(txn, dbi, &compareKeys);

    uint64_t numberOfEntries{0};
    MDB_stat stat;
    if (!mdb_stat(txn, dbi, &stat)) {
      numberOfEntries = stat.ms_entries;
    }
    if (VERBOSE) {
      std::cerr << "[" << ARGV0 << "]: Found " << numberOfEntries << " entries in database 'all' in " << CABINET << std::endl;
    }

    MDB_cursor *cursor;
    if (!(retCode = mdb_cursor_open(txn, dbi, &cursor))) {
      int32_t oldPercentage{-1};
      MDB_val key;
      MDB_val val;

      if (START_IN_NS > 0) {
        const uint64_t MAXKEYSIZE = 511;
        std::vector<char> _key;
        _key.reserve(MAXKEYSIZE);

        cabinet::Key query;
        query.timeStamp(START_IN_NS);
        
        key.mv_size = setKey(query, _key.data(), _key.capacity());
        key.mv_data = _key.data();

        if (!checkErrorCode(mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE), __LINE__, "mdb_cursor_get")) {
          return retCode;
        }
      }

      cabinet::Key storedKey;
      while ((retCode = mdb_cursor_get(cursor, &key, &val, MDB_NEXT_NODUP)) == 0) {
        bool print{mapOfEnvelopesToExport.size() == 0};

        const char *ptr = static_cast<char*>(key.mv_data);
        storedKey = getKey(ptr, key.mv_size);

        // Out of range.
        if ( (END_IN_NS > 0) && (storedKey.timeStamp() > END_IN_NS) ) {
          break;
        }

        if (VERBOSE) {
          std::cerr << storedKey.timeStamp() << ": " << storedKey.dataType() << "/" << storedKey.senderStamp() << std::endl;
        }

        if (mapOfEnvelopesToExport.size() > 0) {
          std::stringstream sstr;
          sstr << storedKey.dataType() << "/" << storedKey.senderStamp();
          std::string str = sstr.str();
          print = (mapOfEnvelopesToExport.count(str) > 0);
        }

        if (print) {
          if (storedKey.length() > val.mv_size) {
            std::vector<char> decompressedValue;
            decompressedValue.reserve(storedKey.length());
            const ssize_t decompressedSize = LZ4_decompress_safe(static_cast<char*>(val.mv_data), decompressedValue.data(), val.mv_size, decompressedValue.capacity());
            std::cout.write(static_cast<char*>(decompressedValue.data()), decompressedSize);
          }
          else {
            // Value to dump is uncompressed.
            std::cout.write(static_cast<char*>(val.mv_data), val.mv_size);
          }
          std::cout.flush();
          entries++;
        }

        if (START == 0) {
          const int32_t percentage = static_cast<int32_t>(static_cast<float>(entries * 100.0f) / static_cast<float>(numberOfEntries));
          if (((percentage % 5 == 0) && (percentage != oldPercentage)) && VERBOSE) {
            std::cerr << "[" << ARGV0 << "]: Processed " << percentage << "% (" << entries << " entries) from " << CABINET << "." << std::endl;
            oldPercentage = percentage;
          }
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
  std::cerr << "[" << ARGV0 << "]: Exported " << entries << " entries from " << CABINET << "." << std::endl;
  return retCode;
}

#endif
