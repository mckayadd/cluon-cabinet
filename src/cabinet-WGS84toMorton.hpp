/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef CABINETWGS84TOMORTON_HPP
#define CABINETWGS84TOMORTON_HPP

#include "cluon-complete.hpp"
#include "opendlv-standard-message-set.hpp"
#include "key.hpp"
#include "morton.hpp"
#include "lmdb++.h"
#include "lz4.h"

#include <iostream>
#include <sstream>
#include <string>

inline bool cabinet_WGS84toMorton(const uint64_t &MEM, const std::string &CABINET, const std::string &MORTONCABINET, const bool &VERBOSE) {
  bool failed{false};
  try {
    auto env = lmdb::env::create();
    env.set_mapsize(MEM/2 * 1024UL * 1024UL * 1024UL);
    env.set_max_dbs(100);
    env.open(CABINET.c_str(), MDB_NOSUBDIR, 0600);

    auto envout = lmdb::env::create();
    envout.set_mapsize(MEM/2 * 1024UL * 1024UL * 1024UL);
    envout.set_max_dbs(100);
    envout.open(MORTONCABINET.c_str(), MDB_NOSUBDIR, 0600);

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
    while (cursor.get(&key, &value, MDB_NEXT)) {
      entries++;
      const char *ptr = static_cast<char*>(key.mv_data);
      cabinet::Key storedKey = getKey(ptr, key.mv_size);
      if (storedKey.dataType() == opendlv::proxy::GeodeticWgs84Reading::ID()) {
        std::vector<char> val;
        val.reserve(storedKey.length());
        if (storedKey.length() > value.mv_size) {
          LZ4_decompress_safe(static_cast<char*>(value.mv_data), val.data(), value.mv_size, val.capacity());
        }
        else {
          // Stored value is uncompressed.
          // recFile.write(static_cast<char*>(val.mv_data), val.mv_size);
          memcpy(val.data(), static_cast<char*>(value.mv_data), value.mv_size);
        }
        std::stringstream sstr{std::string(val.data(), storedKey.length())};
        auto e = cluon::extractEnvelope(sstr);
        if (e.first) {
          // Compose name for database.
          std::stringstream _dataType_senderStamp;
          _dataType_senderStamp << opendlv::proxy::GeodeticWgs84Reading::ID() << '/'<< e.second.senderStamp() << "-morton";
          const std::string _shortKey{_dataType_senderStamp.str()};

          // Extract value from Envelope and compute Morton code.
          const auto tmp = cluon::extractMessage<opendlv::proxy::GeodeticWgs84Reading>(std::move(e.second));
          auto morton = convertLatLonToMorton(std::make_pair(tmp.latitude(), tmp.longitude()));
          //std::cerr << tmp.latitude() << ", " << tmp.longitude() << " = " << morton << ", " << storedKey.timeStamp() << std::endl;

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
      }

      const int32_t percentage = static_cast<int32_t>((static_cast<float>(entries) * 100.0f) / static_cast<float>(totalEntries));
      if ((percentage % 5 == 0) && (percentage != oldPercentage)) {
        std::clog <<"Processed " << percentage << "% (" << entries << " entries) from " << CABINET << std::endl;
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
