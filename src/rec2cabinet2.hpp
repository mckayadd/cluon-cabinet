/*
 * Copyright (C) 2022  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef REC2CABINET2_HPP
#define REC2CABINET2_HPP

#include "cluon-complete.hpp"
#include "key.hpp"
#include "db.hpp"
#include "in-ranges.hpp"

#include "lmdb++.h"
#include "lz4.h"
#include "lz4hc.h"
#include "xxhash.h"

#include <cstdio>
#include <cstring>
#include <cstdint>

#include <iostream>
#include <iomanip>
#include <locale>
#include <string>
#include <vector>

inline int rec2cabinet(const std::string &ARGV0, const uint64_t &MEM, const std::string &REC, const std::string &CABINET, const uint64_t &USERDATA, cluon::In_Ranges<int64_t> ranges,  const bool &VERBOSE) {
  int32_t retCode{0};
  const int numberOfDatabases{100};
  const int64_t SIZE_DB = MEM * 1024UL * 1024UL * 1024UL;
  const uint64_t MAXKEYSIZE = 511;
  try {
    auto env = lmdb::env::create();
    env.set_mapsize(SIZE_DB);
    env.set_max_dbs(numberOfDatabases);
    env.open(CABINET.c_str(), MDB_NOSUBDIR, 0600);

    {
      auto rotxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
      try {
        auto dbAll = lmdb::dbi::open(rotxn, "all");
        dbAll.set_compare(rotxn, &compareKeys);
        const uint64_t totalEntries = dbAll.size(rotxn);
        std::clog << "[" << ARGV0 << "]: Found " << totalEntries << " entries in table 'all' in " << CABINET << std::endl;
      }
      catch(...) {
        std::clog << "[" << ARGV0 << "]: No table 'all' found in " << CABINET << ", will be created on opening." << std::endl;
      }
    }

    const XXH32_hash_t hashOfFilename = XXH32(REC.c_str(), REC.size(), 0);

    const bool AUTO_REWIND{false};
    const bool THREADING{false};

    const cluon::data::TimeStamp BEFORE{cluon::time::now()};
    cluon::Player player(REC, AUTO_REWIND, THREADING);
    uint32_t entries{0};
    const uint32_t totalEntries{player.totalNumberOfEnvelopesInRecFile()};

    auto txn = lmdb::txn::begin(env);
    auto dbAll = lmdb::dbi::open(txn, "all", MDB_CREATE);
    dbAll.set_compare(txn, &compareKeys);

    while (player.hasMoreData()) {
      auto entry = player.getNextEnvelopeToBeReplayed();
      if (entry.first) {
        entries++;

        cluon::data::Envelope e{std::move(entry.second)};
        auto sampleTimeStamp{cluon::time::toMicroseconds(e.sampleTimeStamp())};

        if (!ranges.empty() && !(ranges.isInAnyRange(sampleTimeStamp * 1000UL))) {
          // This Envelope resides temporally not within any allowed start/end range.
          continue;
        }

        cabinet::Key k;
        k.dataType(e.dataType())
          .senderStamp(e.senderStamp())
          .hashOfRecFile(hashOfFilename)
          .userData(USERDATA)
          .version(0);

        // Create bytes to store in "all".
        std::string sVal{cluon::serializeEnvelope(std::move(e))};
        char *ptrToValue = const_cast<char*>(sVal.data());
        ssize_t lengthOfValue = sVal.size();

        XXH64_hash_t hash = XXH64(sVal.data(), sVal.size(), 0);
        k.hash(hash)
         .length(sVal.size());

        // Compress value via lz4.
        std::vector<char> compressedValue;
        ssize_t compressedSize{0};
        {
          ssize_t expectedCompressedSize = LZ4_compressBound(sVal.size());
          compressedValue.reserve(expectedCompressedSize);
          //compressedSize = LZ4_compress_default(sVal.data(), compressedValue.data(), sVal.size(), compressedValue.capacity());
          compressedSize = LZ4_compress_HC(sVal.data(), compressedValue.data(), sVal.size(), compressedValue.capacity(), LZ4HC_CLEVEL_MAX);
          if (VERBOSE) {
            std::clog << "lz4 actual size: " << compressedSize << std::endl;
          }
          if ( (compressedSize > 0) && (compressedSize < lengthOfValue) ) {
            ptrToValue = compressedValue.data();
            lengthOfValue = compressedSize;
          }
        }

        std::vector<char> _key;
        _key.reserve(MAXKEYSIZE);

        MDB_val key;
        MDB_val value;
        int64_t sampleTimeStampOffsetToAvoidCollision{0};
        do {
          k.timeStamp(sampleTimeStamp * 1000UL + sampleTimeStampOffsetToAvoidCollision);

          key.mv_size = setKey(k, _key.data(), _key.capacity());
          key.mv_data = _key.data();

          value.mv_size = lengthOfValue;
          value.mv_data = ptrToValue;

#if 0
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
#endif
         
          // Try next slot if already taken.
          sampleTimeStampOffsetToAvoidCollision++;
        } while ( MDB_KEYEXIST == (retCode = lmdb::dbi_put(txn, dbAll, &key, &value, MDB_NOOVERWRITE)) );

      }
    }

    txn.commit();

    const cluon::data::TimeStamp AFTER{cluon::time::now()};
    std::clog << "[" << ARGV0 << "]: Processed 100% (" << entries << " entries) from " << REC
              << " in " << cluon::time::deltaInMicroseconds(AFTER, BEFORE) / static_cast<int64_t>(1000 * 1000) << "s." << std::endl;
  }
  catch(...) {
    retCode = 1;
  }

  return retCode;
#if 0

  // Iterate through .rec file and fill database.
  {
    std::fstream recFile;
    recFile.open(REC.c_str(), std::ios_base::in | std::ios_base::binary);

    if (recFile.good()) {
      const XXH32_hash_t hashOfFilename = XXH32(REC.c_str(), REC.size(), 0);
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

            if (!ranges.empty() && !(ranges.isInAnyRange(sampleTimeStamp * 1000UL))) {
              // This Envelope resides temporally not within any allowed start/end range.
              continue;
            }

            // Create bytes to store in "all".
            std::string sVal{cluon::serializeEnvelope(std::move(e))};
            char *ptrToValue = const_cast<char*>(sVal.data());
            ssize_t lengthOfValue = sVal.size();

            XXH64_hash_t hash = XXH64(sVal.data(), sVal.size(), 0);
            if (VERBOSE) {
              std::clog << "hash: " << std::hex << "0x" << hash << std::dec << ", value size = " << sVal.size() << std::endl;
            }
            // Compress value via lz4.
            std::vector<char> compressedValue;
            ssize_t compressedSize{0};
            {
              ssize_t expectedCompressedSize = LZ4_compressBound(sVal.size());
              compressedValue.reserve(expectedCompressedSize);
              //compressedSize = LZ4_compress_default(sVal.data(), compressedValue.data(), sVal.size(), compressedValue.capacity());
              compressedSize = LZ4_compress_HC(sVal.data(), compressedValue.data(), sVal.size(), compressedValue.capacity(), LZ4HC_CLEVEL_MAX);
              if (VERBOSE) {
                std::clog << "lz4 actual size: " << compressedSize << std::endl;
              }
              if ( (compressedSize > 0) && (compressedSize < lengthOfValue) ) {
                ptrToValue = compressedValue.data();
                lengthOfValue = compressedSize;
              }
            }

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

            cabinet::Key k;
            k.dataType(e.dataType())
              .senderStamp(e.senderStamp())
              .hash(hash)
              .hashOfRecFile(hashOfFilename)
              .length(sVal.size())
              .userData(USERDATA)
              .version(0);

            MDB_val key;
            MDB_val value;
            int64_t sampleTimeStampOffsetToAvoidCollision{0};
            do {
              k.timeStamp(sampleTimeStamp * 1000UL + sampleTimeStampOffsetToAvoidCollision);

              key.mv_size = setKey(k, _key.data(), _key.capacity());
              key.mv_data = _key.data();

              value.mv_size = lengthOfValue;
              value.mv_data = ptrToValue;

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

            // Add key to separate database named "dataType/senderStamp".
            {
              MDB_txn *_txn{nullptr};
              if (nullptr == _txn) {
                if (!checkErrorCode(mdb_txn_begin(env, nullptr, 0, &_txn), __LINE__, "mdb_txn_begin")) {
                  mdb_env_close(env);
                  return 1;
                }
              }

              std::stringstream _dataType_senderStamp;
              _dataType_senderStamp << e.dataType() << '/'<< e.senderStamp();
              const std::string _shortKey{_dataType_senderStamp.str()};

              // Make sure to have a database "dataType/senderStamp" and that we have it open.
              MDB_dbi dbDataTypeSenderStamp{0}; 
              if (!checkErrorCode(mdb_dbi_open(_txn, _shortKey.c_str(), MDB_CREATE, &dbDataTypeSenderStamp), __LINE__, "mdb_dbi_open")) {
                mdb_txn_abort(_txn);
                mdb_env_close(env);
                break;
              }
              mdb_set_compare(_txn, dbDataTypeSenderStamp, &compareKeys);

              key.mv_size = setKey(k, _key.data(), _key.capacity());
              key.mv_data = _key.data();

              value.mv_size = 0;
              value.mv_data = nullptr;

              if (MDB_SUCCESS != (retCode = mdb_put(_txn, dbDataTypeSenderStamp, &key, &value, 0))) {
                std::cerr << ARGV0 << ": " << "mdbx_put: (" << retCode << ") " << mdb_strerror(retCode) << std::endl;
              }

              // Commit write.
              {
                if (MDB_SUCCESS != (retCode = mdb_txn_commit(_txn))) {
                  std::cerr << ARGV0 << ": " << "mdb_txn_commit: (" << retCode << ") " << mdb_strerror(retCode) << std::endl;
                  mdb_env_close(env);
                  break;
                }
                _txn = nullptr;
                mdb_dbi_close(env, dbDataTypeSenderStamp);
              }
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
    }
    else {
      std::clog << "[" << ARGV0 << "]: " << REC << " could not be opened." << std::endl;
    }
    mdb_env_sync(env, true);
  }

  printNumberOfEntries();

  if (env) {
    mdb_env_close(env);
  }
  return retCode;
#endif
}

#endif
