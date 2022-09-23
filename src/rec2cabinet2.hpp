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
        lmdb::dbi_close(env, dbAll);
      }
      catch(...) {
        std::clog << "[" << ARGV0 << "]: No table 'all' found in " << CABINET << ", will be created on opening." << std::endl;
      }
      rotxn.abort();
    }

    const XXH32_hash_t hashOfFilename = XXH32(REC.c_str(), REC.size(), 0);

    const cluon::data::TimeStamp BEFORE{cluon::time::now()};
    uint32_t entries{0};
    std::fstream recFile;
    recFile.open(REC.c_str(), std::ios_base::in | std::ios_base::binary);

    if (recFile.good()) {
      auto txn = lmdb::txn::begin(env);
      auto dbAll = lmdb::dbi::open(txn, "all", MDB_CREATE);
      dbAll.set_compare(txn, &compareKeys);

      uint64_t totalBytesRead = 0;
      // Determine file size to display progress.
      recFile.seekg(0, recFile.end);
      int64_t fileLength = recFile.tellg();
      recFile.seekg(0, recFile.beg);

      int32_t oldPercentage{-1};
      while (recFile.good()) {
        const uint64_t POS_BEFORE = static_cast<uint64_t>(recFile.tellg());
        auto retVal               = cluon::extractEnvelope(recFile);
        const uint64_t POS_AFTER  = static_cast<uint64_t>(recFile.tellg());

        if (!recFile.eof() && retVal.first) {
          totalBytesRead += (POS_AFTER - POS_BEFORE);

          cluon::data::Envelope e{std::move(retVal.second)};
          auto sampleTimeStamp{cluon::time::toMicroseconds(e.sampleTimeStamp())};

          if (!ranges.empty() && !(ranges.isInAnyRange(sampleTimeStamp * 1000UL))) {
            // This Envelope resides temporally not within any allowed start/end range.
            if (VERBOSE) {
              std::cerr << "not in range: " << sampleTimeStamp << std::endl;
            }
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
            compressedSize = LZ4_compress_HC(sVal.data(), compressedValue.data(), sVal.size(), compressedValue.capacity(), LZ4HC_CLEVEL_MAX);
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
          bool duplicate{false};
          do {
            k.timeStamp(sampleTimeStamp * 1000UL + sampleTimeStampOffsetToAvoidCollision);

            key.mv_size = setKey(k, _key.data(), _key.capacity());
            key.mv_data = _key.data();

            value.mv_size = lengthOfValue;
            value.mv_data = ptrToValue;

            // Check for duplicated entries.
            {
              try {
                duplicate = false;
                auto _rotxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
                //auto _dbAll = lmdb::dbi::open(_rotxn, "all");
                //_dbAll.set_compare(txn, &compareKeys);
                auto _cursor = lmdb::cursor::open(_rotxn, dbAll);
                MDB_val tmpKey = key;
                MDB_val tmpVal;
                //if (MDB_SUCCESS == _cursor.get(&tmpKey, &tmpVal, MDB_SET_KEY)) {
                if (MDB_SUCCESS == mdb_cursor_get(_cursor, &tmpKey, &tmpVal, MDB_SET_KEY)) {
                  // Extract xxhash from found key and compare with calculated key to maybe skip adding this value.
                  const char *ptr = static_cast<char*>(tmpKey.mv_data);
                  cabinet::Key storedKey = getKey(ptr, tmpKey.mv_size);
                  duplicate = (hash == storedKey.hash());
                  if (VERBOSE) {
                    std::cerr << std::hex << "hash-to-store: 0x" << hash << ", hash-stored: 0x" << storedKey.hash() << std::dec << ", is duplicate = " << duplicate << std::endl;
                  }
                }
                _cursor.close();
                //lmdb::dbi_close(env, _dbAll);
                _rotxn.abort();
                if (duplicate) {
                  // value is existing, skip storing
                  retCode = 0;
                  break;
                }
              }
              catch(...) {
              }
            }

            // Try next slot if already taken.
            sampleTimeStampOffsetToAvoidCollision++;
          } while ( MDB_KEYEXIST == (retCode = lmdb::dbi_put2(txn, dbAll, &key, &value, MDB_NOOVERWRITE)) );
          if (MDB_SUCCESS == retCode) {
            entries++;
          }

          // Add key to separate database named "dataType/senderStamp".
          if (!duplicate) {
            std::stringstream _dataType_senderStamp;
            _dataType_senderStamp << e.dataType() << '/'<< e.senderStamp();
            const std::string _shortKey{_dataType_senderStamp.str()};

            // Make sure to have a database "dataType/senderStamp" and that we have it open.
            auto dbDataTypeSenderStamp = lmdb::dbi::open(txn, _shortKey.c_str(), MDB_CREATE);
            dbDataTypeSenderStamp.set_compare(txn, &compareKeys);

            key.mv_size = setKey(k, _key.data(), _key.capacity());
            key.mv_data = _key.data();

            value.mv_size = 0;
            value.mv_data = nullptr;

            lmdb::dbi_put(txn, dbDataTypeSenderStamp, &key, &value, 0);
          }

          const int32_t percentage = static_cast<int32_t>((static_cast<float>(recFile.tellg()) * 100.0f) / static_cast<float>(fileLength));
          if ((percentage % 5 == 0) && (percentage != oldPercentage)) {
            std::clog << "[" << ARGV0 << "]: Processed " << percentage << "% (" << entries << " entries) from " << REC << std::endl;
            oldPercentage = percentage;
          }
        }
      }

      txn.commit();
    }

    const cluon::data::TimeStamp AFTER{cluon::time::now()};
    std::clog << "[" << ARGV0 << "]: Processed 100% (" << entries << " entries) from " << REC
              << " in " << cluon::time::deltaInMicroseconds(AFTER, BEFORE) / static_cast<int64_t>(1000 * 1000) << "s." << std::endl;
  }
  catch(...) {
    retCode = 1;
  }

  return retCode;
}

#endif
