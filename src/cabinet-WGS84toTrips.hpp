/*
 * Copyright (C) 2022  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef CABINETWGS84TOTRIPS_HPP
#define CABINETWGS84TOTRIPS_HPP

#include "cluon-complete.hpp"
#include "opendlv-standard-message-set.hpp"
#include "key.hpp"
#include "morton.hpp"
#include "lmdb++.h"
#include "lz4.h"
#include "geofence.hpp"

#include <iostream>
#include <sstream>
#include <string>

inline bool cabinet_WGS84toTrips(const uint64_t &MEM, const std::string &CABINET, const uint32_t &senderStamp, std::vector<std::array<double,2>> polygon, const std::string &TRIPSCABINET, const bool &VERBOSE) {
  bool failed{false};
  try {
    auto env = lmdb::env::create();
    env.set_mapsize(MEM/2 * 1024UL * 1024UL * 1024UL);
    env.set_max_dbs(100);
    env.open(CABINET.c_str(), MDB_NOSUBDIR, 0600);

    auto envout = lmdb::env::create();
    envout.set_mapsize(MEM/2 * 1024UL * 1024UL * 1024UL);
    envout.set_max_dbs(100);
    envout.open(TRIPSCABINET.c_str(), MDB_NOSUBDIR, 0600);

    // Fetch key/value pairs in a read-only transaction.
    auto rotxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
    auto dbi = lmdb::dbi::open(rotxn, "all");
    dbi.set_compare(rotxn, &compareKeys);
    const uint64_t totalEntries = dbi.size(rotxn);
    std::cerr << "Found " << totalEntries << " entries." << std::endl;
    auto cursor = lmdb::cursor::open(rotxn, dbi);
    MDB_val key;
    MDB_val value;
    int32_t oldPercentage{-1};
    uint64_t entries{0};
    uint64_t skipped{0};
    uint64_t kept{0};
    bool firstValidGPSLocationStored = false;
    std::vector<std::pair<std::vector<char>, std::vector<char> > > bufferOfKeyValuePairsToStore;
    while (cursor.get(&key, &value, MDB_NEXT)) {
      entries++;
      const char *ptr = static_cast<char*>(key.mv_data);
      cabinet::Key storedKey = getKey(ptr, key.mv_size);
      std::vector<char> val;
      val.reserve(storedKey.length());
      if (storedKey.length() > value.mv_size) {
        LZ4_decompress_safe(static_cast<char*>(value.mv_data), val.data(), value.mv_size, val.capacity());
      }
      else {
        // Stored value is uncompressed.
        memcpy(val.data(), static_cast<char*>(value.mv_data), value.mv_size);
      }

      if (  (storedKey.dataType() == opendlv::proxy::GeodeticWgs84Reading::ID())
         && (storedKey.senderStamp() == senderStamp) ) {
        std::stringstream sstr{std::string(val.data(), storedKey.length())};
        auto e = cluon::extractEnvelope(sstr);
        if (e.first) {
          // Extract value from Envelope and check whether location is within polygon.
          const auto tmp = cluon::extractMessage<opendlv::proxy::GeodeticWgs84Reading>(std::move(e.second));
          std::array<double,2> pos{tmp.latitude(),tmp.longitude()};
          bool inside = geofence::isIn<double>(polygon, pos);
          if (VERBOSE) {
            std::cout << tmp.latitude() << "," << tmp.longitude() << ": " << inside << std::endl;
          }

          if (inside) {
            // Only store buffered key/value pairs after the first valid GPS location has been stored.
            if (firstValidGPSLocationStored) {
              // Firstly, all message that are buffered so far must be stored in the database.
              for (auto f : bufferOfKeyValuePairsToStore) {
                // Store buffered key/value in database "all".
                {
                  MDB_val _key;
                  _key.mv_size = f.first.size();
                  _key.mv_data = f.first.data();

                  MDB_val _value;
                  _value.mv_size = f.second.size();
                  _value.mv_data = f.second.data();

                  auto txn = lmdb::txn::begin(envout);
                  auto dbAll = lmdb::dbi::open(txn, "all", MDB_CREATE);
                  dbAll.set_compare(txn, &compareKeys);
                  lmdb::dbi_put(txn, dbAll.handle(), &_key, &_value, 0); 
                  txn.commit();
                }

                // Store key also in database datatype/senderStamp:
                {
                  MDB_val __key;
                  __key.mv_size = f.first.size();
                  __key.mv_data = f.first.data();

                  MDB_val __value;
                  __value.mv_size = 0;
                  __value.mv_data = nullptr;

                  const char *__ptr = static_cast<char*>(__key.mv_data);
                  cabinet::Key __storedKey = getKey(__ptr, __key.mv_size);

                   std::stringstream _dataType_senderStamp;
                  _dataType_senderStamp << __storedKey.dataType() << '/'<< __storedKey.senderStamp();
                  const std::string _shortKey{_dataType_senderStamp.str()};

                  auto txn = lmdb::txn::begin(envout);
                  auto dbDataTypeSenderStamp = lmdb::dbi::open(txn, _shortKey.c_str(), MDB_CREATE);
                  dbDataTypeSenderStamp.set_compare(txn, &compareKeys);
                  lmdb::dbi_put(txn, dbDataTypeSenderStamp.handle(), &__key, &__value, 0); 
                  txn.commit();
                }
              }
            }

            // Buffer has been added to database and can be cleared now.
            if (!firstValidGPSLocationStored) {
              skipped += bufferOfKeyValuePairsToStore.size();
            }
            else {
              kept += bufferOfKeyValuePairsToStore.size();
            }
            bufferOfKeyValuePairsToStore.clear();

            // Finally, store the current key/value pair.
            {
              {
                auto txn = lmdb::txn::begin(envout);
                auto dbAll = lmdb::dbi::open(txn, "all", MDB_CREATE);
                dbAll.set_compare(txn, &compareKeys);
                lmdb::dbi_put(txn, dbAll.handle(), &key, &value, 0); 
                txn.commit();
              }

              // Store key also in database datatype/senderStamp:
              {
                MDB_val __value;
                __value.mv_size = 0;
                __value.mv_data = nullptr;

                 std::stringstream _dataType_senderStamp;
                _dataType_senderStamp << storedKey.dataType() << '/'<< storedKey.senderStamp();
                const std::string _shortKey{_dataType_senderStamp.str()};

                auto txn = lmdb::txn::begin(envout);
                auto dbDataTypeSenderStamp = lmdb::dbi::open(txn, _shortKey.c_str(), MDB_CREATE);
                dbDataTypeSenderStamp.set_compare(txn, &compareKeys);
                lmdb::dbi_put(txn, dbDataTypeSenderStamp.handle(), &key, &__value, 0); 
                txn.commit();
              }

              // Compute Morton code from WGS84 coordinate to allow for geographical queries.
              {
                std::stringstream _dataType_senderStamp;
                _dataType_senderStamp << opendlv::proxy::GeodeticWgs84Reading::ID() << '/'<< e.second.senderStamp() << "-morton";
                const std::string _shortKey{_dataType_senderStamp.str()};

                auto morton = convertLatLonToMorton(std::make_pair(tmp.latitude(), tmp.longitude()));
                if (VERBOSE) {
                  std::cerr << tmp.latitude() << ", " << tmp.longitude() << " = " << morton << ", " << storedKey.timeStamp() << std::endl;
                }

                // Store data.
                auto txn = lmdb::txn::begin(envout);
                auto dbGeodeticWgs84SenderStamp = lmdb::dbi::open(txn, _shortKey.c_str(), MDB_CREATE|MDB_DUPSORT|MDB_DUPFIXED );
                dbGeodeticWgs84SenderStamp.set_compare(txn, &compareMortonKeys);
                lmdb::dbi_set_dupsort(txn, dbGeodeticWgs84SenderStamp.handle(), &compareKeys);
                {
                  // key is the morton code in network byte order
                  MDB_val __key;
                  __key.mv_size = sizeof(morton);
                  morton = htobe64(morton);
                  __key.mv_data = &morton;

                  // value is the nanosecond timestamp in network byte order of the entry from table 'all'
                  MDB_val __value;
                  int64_t _timeStamp = storedKey.timeStamp();
                  _timeStamp = htobe64(_timeStamp);
                  __value.mv_size = sizeof(_timeStamp);
                  __value.mv_data = &_timeStamp;

                  lmdb::dbi_put(txn, dbGeodeticWgs84SenderStamp.handle(), &__key, &__value, 0); 
                }
                txn.commit();
              }

              firstValidGPSLocationStored = true;
              kept++;
            }
          }
          else {
            // Clear buffered key/value pairs as this GPS location is outside the last that was within.
            skipped += bufferOfKeyValuePairsToStore.size();
            bufferOfKeyValuePairsToStore.clear();
          }
        }
      }
      else {
        // This data sample occurred temporally after the last written GPS location that was inside the geofence.
        // Hence, we can only dump this data sample when the next GPS location that comes is also inside the geofence.
        // Therefore, we buffer for now.
        std::vector<char> keyToBuffer;
        keyToBuffer.reserve(key.mv_size);
        std::copy(static_cast<char*>(key.mv_data), static_cast<char*>(key.mv_data) + key.mv_size, std::back_inserter(keyToBuffer));

        std::vector<char> valueToBuffer;
        valueToBuffer.reserve(value.mv_size);
        std::copy(static_cast<char*>(value.mv_data), static_cast<char*>(value.mv_data) + key.mv_size, std::back_inserter(valueToBuffer));

        auto p = std::make_pair(keyToBuffer, valueToBuffer);
        bufferOfKeyValuePairsToStore.push_back(p);
      }

      const int32_t percentage = static_cast<int32_t>((static_cast<float>(entries) * 100.0f) / static_cast<float>(totalEntries));
      if ((percentage % 5 == 0) && (percentage != oldPercentage)) {
        std::clog <<"Processed " << percentage << "% (" << entries << " entries, kept: " << kept << ", skipped: " << skipped << ") from " << CABINET << std::endl;
        oldPercentage = percentage;
      }
    }
    cursor.close();
    rotxn.abort();
  }
  catch (...) {
    failed = true;
  }
  return failed;
}

#endif
