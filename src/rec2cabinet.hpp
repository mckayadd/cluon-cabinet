/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef REC2CABINET_HPP
#define REC2CABINET_HPP

#include "cluon-complete.hpp"
#include "key.hpp"
#include "db.hpp"
#include "lmdb.h"
#include "xxhash.h"

#include <cstdio>
#include <cstring>
#include <cstdint>

#include <iostream>
#include <iomanip>
#include <locale>
#include <string>
#include <vector>

inline int rec2cabinet(const std::string &ARGV0, const std::string &REC, const std::string &CABINET, const bool &VERBOSE) {
  int32_t retCode{0};
  MDB_env *env{nullptr};
  const int numberOfDatabases{100};
  const int64_t SIZE_DB = 64UL *1024UL * 1024UL * 1024UL * 1024UL;
  const uint64_t MAXKEYSIZE = 511;

  // lambda to check the interaction with the database.
  auto checkErrorCode = [argv0=ARGV0](int32_t rc, int32_t line, std::string caller) {
    if (0 != rc) {
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
  if (!checkErrorCode(mdb_env_open(env, CABINET.c_str(), MDB_NOSUBDIR, 0600), __LINE__, "mdb_env_open")) {
    mdb_env_close(env);
    return 1;
  }

  {
    MDB_txn *txn{nullptr};
    MDB_dbi dbi{0};
    if (!checkErrorCode(mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn), __LINE__, "mdb_txn_begin")) {
      mdb_env_close(env);
      return 1;
    }
    retCode = mdb_dbi_open(txn, "all", 0/*no flags*/, &dbi);
    if (MDB_NOTFOUND  == retCode) {
      std::clog << "[" << ARGV0 << "]: No table 'all' found in " << CABINET << ", will be created on opening." << std::endl;
    }
    else {
      uint64_t numberOfEntries{0};
      MDB_stat stat;
      if (!mdb_stat(txn, dbi, &stat)) {
        numberOfEntries = stat.ms_entries;
      }
      std::clog << "[" << ARGV0 << "]: Found " << numberOfEntries << " entries in table 'all' in " << CABINET << std::endl;
    }
    mdb_txn_abort(txn);
    if (dbi) {
      mdb_dbi_close(env, dbi);
    }
  }

  // Iterate through .rec file and fill database.
  {
    std::fstream recFile;
    recFile.open(REC.c_str(), std::ios_base::in | std::ios_base::binary);

    if (recFile.good()) {
      uint32_t entries{0};
      uint64_t totalBytesRead = 0;

      // Determine file size to display progress.
      recFile.seekg(0, recFile.end);
      int64_t fileLength = recFile.tellg();
      recFile.seekg(0, recFile.beg);

      // Read complete file and store file positions to Envelopes to create
      // index of available data. The actual reading of Envelopes is deferred.
      const cluon::data::TimeStamp BEFORE{cluon::time::now()};
      {
        int32_t oldPercentage{-1};
        while (recFile.good()) {
          const uint64_t POS_BEFORE = static_cast<uint64_t>(recFile.tellg());
          auto retVal               = cluon::extractEnvelope(recFile);
          const uint64_t POS_AFTER  = static_cast<uint64_t>(recFile.tellg());

          if (!recFile.eof() && retVal.first) {
            entries++;
            totalBytesRead += (POS_AFTER - POS_BEFORE);

            cluon::data::Envelope e{std::move(retVal.second)};
            auto sampleTimeStamp{cluon::time::toMicroseconds(e.sampleTimeStamp())};
            // Create bytes to store in "all".
            const std::string sVal{cluon::serializeEnvelope(std::move(e))};

            XXH64_hash_t hash = XXH64(sVal.c_str(), sVal.size(), 0);
            if (VERBOSE) {
              std::clog << "hash: " << std::hex << "0x" << hash << std::dec << ", value size = " << sVal.size() << std::endl;
            }
/*
            // Compress value using zstd.
            std::string compressedValue{};
            if (COMPRESS) {
              size_t estimatedCompressedSize{ZSTD_compressBound(sVal.size())};
              compressedValue.resize(estimatedCompressedSize);

              constexpr const int LEVEL{22};
              auto compressedSize = ZSTD_compress((void*)compressedValue.data(), estimatedCompressedSize, sVal.data(), sVal.size(), LEVEL);

              compressedValue.resize(compressedSize);
              compressedValue.shrink_to_fit();

              value.mv_size = compressedValue.size();
              value.mv_data = const_cast<char*>(compressedValue.c_str());
            }
*/
            std::vector<char> _key;
            _key.reserve(MAXKEYSIZE);
            
            MDB_dbi dbAll{0}; 
            MDB_txn *txn{nullptr};

            // No transaction available, create one.
            if (nullptr == txn) {
              if (!checkErrorCode(mdb_txn_begin(env, nullptr, 0, &txn), __LINE__, "mdb_txn_begin")) {
                mdb_env_close(env);
                return 1;
              }
            }

            // Make sure to have a database "all" and that we have it open.
            if (!checkErrorCode(mdb_dbi_open(txn, "all", MDB_CREATE, &dbAll), __LINE__, "mdb_dbi_open")) {
              mdb_txn_abort(txn);
              mdb_env_close(env);
              break;
            }
            mdb_set_compare(txn, dbAll, &compareKeys);
#if 0
              {
                // version 0:
                // if (511 - (value.mv_size + offset) > 0) --> store value directly in key 
                if ( MAXKEYSIZE > (offset + value.mv_size) )  {
                  // b25-b26: uint16_t: length of the value
                  const uint16_t length = static_cast<uint16_t>(value.mv_size);
                  std::memcpy(_key.data() + offset, reinterpret_cast<const char*>(&length), sizeof(uint16_t));
                  offset += sizeof(uint16_t);

                  std::memcpy(_key.data() + offset, reinterpret_cast<const char*>(value.mv_data), value.mv_size);
                  offset += value.mv_size;
                  value.mv_size = 0;
                  value.mv_data = 0;
                }
                else {
                  // b25-b26: uint16_t: length of the value
                  const uint16_t length = 0;
                  std::memcpy(_key.data() + offset, reinterpret_cast<const char*>(&length), sizeof(uint16_t));
                  offset += sizeof(uint16_t);
                }
              }
#endif
            cabinet::Key k;
            k.dataType(e.dataType())
              .senderStamp(e.senderStamp())
              .hash(hash)
              .version(0)
              .length(sVal.size());

            MDB_val key;
            MDB_val value;
            int64_t sampleTimeStampOffsetToAvoidCollision{0};
            do {
              k.timeStamp(sampleTimeStamp * 1000UL + sampleTimeStampOffsetToAvoidCollision);
              key.mv_size = setKey(k, _key.data(), _key.capacity());
              key.mv_data = _key.data();

              value.mv_size = sVal.size();
              value.mv_data = const_cast<char*>(sVal.c_str());

              // Check for duplicated entries.
              {
                bool duplicate{false};
                MDB_cursor *cursor{nullptr};
                if (MDB_SUCCESS == mdb_cursor_open(txn, dbAll, &cursor)) {
                  // Check if the key exists; if so, retrieve the key and check hash to skip duplicated data.
                  MDB_val tmpKey = key;
                  MDB_val tmpVal;
                  if (MDB_SUCCESS == mdb_cursor_get(cursor, &tmpKey, &tmpVal, MDB_SET_KEY)) {
                    // Extract xxhash from found key and compare with calculated key to maybe skip adding this value.
                    const char *ptr = static_cast<char*>(tmpKey.mv_data);
                    cabinet::Key storedKey = getKey(ptr, tmpKey.mv_size);
                    duplicate = (hash == storedKey.hash());
                    if (VERBOSE) {
                      std::cerr << std::hex << "hash-to-store: 0x" << hash << ", hash-stored: 0x" << storedKey.hash() << std::dec << ", is duplicate = " << duplicate << std::endl;
                    }
                  }
                }
                mdb_cursor_close(cursor);
                if (duplicate) {
                  // value is existing, skip storing
                  retCode = 0;
                  break;
                }
              }
             
              // Try next slot if already taken.
              sampleTimeStampOffsetToAvoidCollision++;
            } while ( MDB_KEYEXIST == (retCode = mdb_put(txn, dbAll, &key, &value, MDB_NOOVERWRITE)) );
            if (0 != retCode) {
              std::cerr << ARGV0 << ": " << "mdb_put: (" << retCode << ") " << mdb_strerror(retCode) << ", stored " << entries << std::endl;
              mdb_txn_abort(txn);
              mdb_env_close(env);
              break;
            }

            // Commit write.
            {
              if (MDB_SUCCESS != (retCode = mdb_txn_commit(txn))) {
                std::cerr << ARGV0 << ": " << "mdb_txn_commit: (" << retCode << ") " << mdb_strerror(retCode) << std::endl;
                mdb_env_close(env);
                break;
              }
              txn = nullptr;
              mdb_dbi_close(env, dbAll);
              dbAll = -1;
            }

            const int32_t percentage = static_cast<int32_t>((static_cast<float>(recFile.tellg()) * 100.0f) / static_cast<float>(fileLength));
            if ((percentage % 5 == 0) && (percentage != oldPercentage)) {
              std::clog << "[" << ARGV0 << "]: Processed " << percentage << "% (" << entries << " entries) from " << REC << std::endl;
              oldPercentage = percentage;
            }
          }
        }
      }
      const cluon::data::TimeStamp AFTER{cluon::time::now()};

      std::clog << "[" << ARGV0 << "]: Processed 100% (" << entries << " entries) from " << REC << "; total bytes read: " << totalBytesRead
                << " in " << cluon::time::deltaInMicroseconds(AFTER, BEFORE) / static_cast<int64_t>(1000 * 1000) << "s." << std::endl;
      {
        MDB_dbi dbAll{0}; 
        MDB_txn *txn{nullptr};

        // No transaction available, create one.
        if (nullptr == txn) {
          if (!checkErrorCode(mdb_txn_begin(env, nullptr, 0, &txn), __LINE__, "mdb_txn_begin")) {
            mdb_env_close(env);
            return 1;
          }
        }

        // Make sure to have a database "all" and that we have it open.
        if (!checkErrorCode(mdb_dbi_open(txn, "all", MDB_CREATE, &dbAll), __LINE__, "mdb_dbi_open")) {
          mdb_txn_abort(txn);
          mdb_env_close(env);
          return 1;
        }
        mdb_set_compare(txn, dbAll, &compareKeys);

        uint64_t numberOfEntries{0};
        MDB_stat stat;
        if (!mdb_stat(txn, dbAll, &stat)) {
          numberOfEntries = stat.ms_entries;
        }
        std::clog << "[" << ARGV0 << "]: Found " << numberOfEntries << " entries in database 'all' in " << CABINET << std::endl;

        mdb_txn_abort(txn);
        mdb_dbi_close(env, dbAll);
      }
    }
    else {
      std::clog << "[" << ARGV0 << "]: " << REC << " could not be opened." << std::endl;
    }
    mdb_env_sync(env, true);
  }
  if (env) {
    mdb_env_close(env);
  }
  return retCode;
}

#endif
