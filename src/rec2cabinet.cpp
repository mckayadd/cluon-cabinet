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
#include <locale>
#include <map>
#include <string>
#include <vector>

typedef unsigned __int128 uint128_t;

struct space_out : std::numpunct<char> {
  char do_thousands_sep()   const { return ','; }  // separate with spaces
  std::string do_grouping() const { return "\3"; } // groups of 3 digit
};

// fcuntion to compare two keys.
// key format:
// 8 bytes as int64_t for timeStamp in nanoseconds
int compareKeys(const MDB_val *a, const MDB_val *b) {
  if (nullptr == a || nullptr == b) {
    return 0;
  }
  if (a->mv_size < sizeof(int64_t) || b->mv_size < sizeof(int64_t)) {
    return 0;
  }
  const int64_t delta{*(static_cast<int64_t*>(a->mv_data)) - *(static_cast<int64_t*>(b->mv_data))};
std::cerr << "cmp: a = " << *(static_cast<int64_t*>(a->mv_data)) << " - b = " << *(static_cast<int64_t*>(b->mv_data)) << " = " << delta << std::endl;
  return (delta < 0 ? -1 : (delta > 0 ? 1 : 0));
};

int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if (0 == commandlineArguments.count("rec")) {
    std::cerr << argv[0] << " transforms a .rec file with Envelopes to an lmdb-based key/value-database." << std::endl;
    std::cerr << "If the specified database exists, the content of the .rec file is added." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --rec=MyFile.rec" << std::endl;
    std::cerr << "         --rec:     name of the recording file" << std::endl;
    std::cerr << "         --cabinet: name of the database file (optional; otherwise, a new file based on the .rec file with .cabinet as suffix is created)" << std::endl;
    std::cerr << "Example: " << argv[0] << " --rec=myFile.rec --cabinet=myStore.cabinet" << std::endl;
    retCode = 1;
  } else {
    std::clog.imbue(std::locale(std::cout.getloc(), new space_out));
    const std::string REC{commandlineArguments["rec"]};
    const std::string CABINET{(commandlineArguments["cabinet"].size() != 0) ? commandlineArguments["cabinet"] : "./" + REC + ".cabinet"};

    MDB_env *env{nullptr};
    const int numberOfDatabases{100};
    const int64_t SIZE_DB = 64UL *1024UL * 1024UL * 1024UL * 1024UL;
    const uint64_t MAXKEYSIZE = 511;

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
        std::clog << "[" << argv[0] << "]: No table 'all' found in " << CABINET << ", will be created on opening." << std::endl;
      }
      else {
        uint64_t numberOfEntries{0};
        MDB_stat stat;
        if (!mdb_stat(txn, dbi, &stat)) {
          numberOfEntries = stat.ms_entries;
        }
        std::clog << "[" << argv[0] << "]: Found " << numberOfEntries << " entries in table 'all' in " << CABINET << std::endl;
      }
      mdb_txn_abort(txn);
      if (dbi) {
        mdb_dbi_close(env, dbi);
      }
    }

    // Iterate through .rec file and fill database.
    {
      std::map<std::string, MDB_dbi> mapOfDatabases{};
      MDB_txn *txn{nullptr};
      MDB_val key;
      MDB_val value;

      uint32_t entries{0};
      uint64_t totalBytesRead = 0;
      uint64_t totalBytesWritten = 0;

      std::fstream recFile;
      recFile.open(REC.c_str(), std::ios_base::in | std::ios_base::binary);

      if (recFile.good()) {
        // Determine file size to display progress.
        recFile.seekg(0, recFile.end);
        int64_t fileLength = recFile.tellg();
        recFile.seekg(0, recFile.beg);

        // Read complete file and store file positions to Envelopes to create
        // index of available data. The actual reading of Envelopes is deferred.
        const cluon::data::TimeStamp BEFORE{cluon::time::now()};
        {
          const uint64_t MAX_BYTES_TO_WRITE{10*1024*1024};
          uint64_t bytesWritten = 0;

          int32_t oldPercentage{-1};
          while (recFile.good()) {
            const uint64_t POS_BEFORE = static_cast<uint64_t>(recFile.tellg());
            auto retVal               = cluon::extractEnvelope(recFile);
            const uint64_t POS_AFTER  = static_cast<uint64_t>(recFile.tellg());

            if (!recFile.eof() && retVal.first) {
              entries++;
              totalBytesRead += (POS_AFTER - POS_BEFORE);

              // Commit write.
              if (bytesWritten > MAX_BYTES_TO_WRITE) {
                bytesWritten = 0;
                if (MDB_SUCCESS != (retCode = mdb_txn_commit(txn))) {
                  std::cerr << argv[0] << ": " << "mdb_txn_commit: (" << retCode << ") " << mdb_strerror(retCode) << std::endl;
                  mdb_env_close(env);
                  break;
                }
                txn = nullptr;
                for (auto it : mapOfDatabases) {
                  mdb_dbi_close(env, it.second);
                }
                mapOfDatabases.clear(); 
              }

              cluon::data::Envelope e{std::move(retVal.second)};
              auto sampleTimeStamp{cluon::time::toMicroseconds(e.sampleTimeStamp())};

              // Create bytes to store in "all".
              const std::string sVal{cluon::serializeEnvelope(std::move(e))};
              value.mv_size = sVal.size();
              value.mv_data = const_cast<char*>(sVal.c_str());
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
              //int64_t _key{0};
              std::vector<char> _key;
              _key.reserve(MAXKEYSIZE);
              
              bytesWritten += value.mv_size + sizeof(_key);
              totalBytesWritten += value.mv_size + sizeof(_key);

              // No transaction available, create one.
              if (nullptr == txn) {
                if (!checkErrorCode(mdb_txn_begin(env, nullptr, 0, &txn), __LINE__, "mdb_txn_begin")) {
                  mdb_env_close(env);
                  return 1;
                }
              }

              // Make sure to have a database "all" and that we have it open.
              if (mapOfDatabases.count("all") == 0) {
                MDB_dbi dbi;
                if (!checkErrorCode(mdb_dbi_open(txn, "all", MDB_CREATE, &dbi), __LINE__, "mdb_dbi_open")) {
                  mdb_txn_abort(txn);
                  mdb_env_close(env);
                  break;
                }
                mapOfDatabases["all"] = dbi;
                mdb_set_compare(txn, mapOfDatabases["all"], &compareKeys);
              }

              // b0-b7: int64_t for timeStamp in nanoseconds
              // b8-b11: int32_t for dataType
              // b12-b15: uint32_t for senderStamp
              // b16: uint8_t: version
              uint16_t offset{sizeof(int64_t) /*field 1: timeStamp in nanoseconds*/};
              {
                int32_t dataType{e.dataType()};
                uint32_t senderStamp{e.senderStamp()};
                uint8_t version{0};
                std::memcpy(_key.data() + offset, reinterpret_cast<const char*>(&dataType), sizeof(int32_t));
                offset += sizeof(int32_t);
                std::memcpy(_key.data() + offset, reinterpret_cast<const char*>(&senderStamp), sizeof(uint32_t));
                offset += sizeof(uint32_t);
                std::memcpy(_key.data() + offset, reinterpret_cast<const char*>(&version), sizeof(uint8_t));
                offset += sizeof(uint8_t);
                {
                  // version 0:
                  // if (511 - (value.mv_size + offset) > 0) --> store value directly in key 
                  if ( MAXKEYSIZE > (offset + value.mv_size) )  {
                    // b17-b18: uint16_t: length of the value
                    uint16_t length = static_cast<uint16_t>(value.mv_size);
                    std::memcpy(_key.data() + offset, reinterpret_cast<const char*>(&length), sizeof(uint16_t));
                    offset += sizeof(uint16_t);

                    std::memcpy(_key.data() + offset, reinterpret_cast<const char*>(value.mv_data), value.mv_size);
                    offset += value.mv_size;
                    value.mv_size = 0;
                    value.mv_data = 0;
                  }
                }
              }

              int64_t sampleTimeStampOffsetToAvoidCollision{0};
              do {
                const int64_t timeStamp = (sampleTimeStamp * 1000UL) + sampleTimeStampOffsetToAvoidCollision;
                std::memcpy(_key.data(), reinterpret_cast<const char*>(&timeStamp), sizeof(int64_t));
                
                key.mv_size = offset;
                key.mv_data = _key.data();
               
                // Try next slot if already taken.
                sampleTimeStampOffsetToAvoidCollision++;
              } while ( MDB_KEYEXIST == (retCode = mdb_put(txn, mapOfDatabases["all"], &key, &value, MDB_NOOVERWRITE)) );
              if (0 != retCode) {
                std::cerr << argv[0] << ": " << "mdb_put: (" << retCode << ") " << mdb_strerror(retCode) << ", stored " << entries << std::endl;
                mdb_txn_abort(txn);
                mdb_env_close(env);
                break;
              }
/*
              // Make sure to have the split database and that we have it open.
              const std::string _shortKey{std::to_string(e.dataType()) + "/" + std::to_string(e.senderStamp())};
              if (mapOfDatabases.count(_shortKey) == 0) {
                MDB_dbi dbi;
                if (!checkErrorCode(mdb_dbi_open(txn, _shortKey.c_str(), MDB_CREATE|MDB_INTEGERKEY , &dbi), __LINE__, "mdb_dbi_open")) {
                  mdb_txn_abort(txn);
                  mdb_env_close(env);
                  break;
                }
                mapOfDatabases[_shortKey] = dbi;
              }
              if (mapOfDatabases.count(_shortKey) == 1) {
                int64_t k = sampleTimeStamp + sampleTimeStampOffsetToAvoidCollision;
                key.mv_size = sizeof(k);
                key.mv_data = &k;

                // value is fwd pointer to key in table "all"
                value.mv_size = sizeof(_key);
                value.mv_data = &_key;

                if (MDB_SUCCESS != (retCode = mdb_put(txn, mapOfDatabases[_shortKey], &key, &value, MDB_NOOVERWRITE))) {
                  std::cerr << argv[0] << ": " << "mdb_put: (" << retCode << ") " << mdb_strerror(retCode) << ", stored " << entries << std::endl;
                  mdb_txn_abort(txn);
                  mdb_env_close(env);
                  break;
                }
              }
*/
              const int32_t percentage = static_cast<int32_t>((static_cast<float>(recFile.tellg()) * 100.0f) / static_cast<float>(fileLength));
              if ((percentage % 5 == 0) && (percentage != oldPercentage)) {
                std::clog << "[" << argv[0] << "]: Processed " << percentage << "% (" << entries << " entries) from " << REC << "; total bytes added: " << totalBytesWritten << std::endl;
                oldPercentage = percentage;
              }
            }
          }
        }
        const cluon::data::TimeStamp AFTER{cluon::time::now()};

        std::clog << "[" << argv[0] << "]: Processed 100% (" << entries << " entries) from " << REC << "; total bytes read: " << totalBytesRead << "; total bytes added: " << totalBytesWritten
                  << " in " << cluon::time::deltaInMicroseconds(AFTER, BEFORE) / static_cast<int64_t>(1000 * 1000) << "s." << std::endl;
        {
          uint64_t numberOfEntries{0};
          MDB_stat stat;
          if (!mdb_stat(txn, mapOfDatabases["all"], &stat)) {
            numberOfEntries = stat.ms_entries;
          }
          std::clog << "[" << argv[0] << "]: Found " << numberOfEntries << " entries in database 'all' in " << CABINET << std::endl;
        }
      }
      else {
        std::clog << "[" << argv[0] << "]: " << REC << " could not be opened." << std::endl;
      }

      if ((nullptr != txn) && (MDB_SUCCESS != (retCode = mdb_txn_commit(txn)))) {
        std::cerr << argv[0] << ": " << "mdb_txn_commit: (" << retCode << ") " << mdb_strerror(retCode) << std::endl;
        mdb_env_close(env);
      }
      for (auto it : mapOfDatabases) {
        mdb_dbi_close(env, it.second);
      }
      mapOfDatabases.clear(); 
      mdb_env_sync(env, true);
    }
    if (env) {
      mdb_env_close(env);
    }
  }
  return retCode;
}
