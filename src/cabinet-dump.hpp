/*
 * Copyright (C) 2022  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef CABINETDUMP_HPP
#define CABINETDUMP_HPP

#include "cluon-complete.hpp"
#include "opendlv-standard-message-set.hpp"
#include "key.hpp"
#include "morton.hpp"
#include "lmdb++.h"
#include "lz4.h"

#include <iostream>
#include <sstream>
#include <string>

inline bool cabinet_dump(const uint64_t &MEM, const std::string &CABINET, const std::string &DB) {
  bool failed{false};
  try {
    auto env = lmdb::env::create();
    env.set_mapsize(MEM * 1024UL * 1024UL * 1024UL);
    env.set_max_dbs(100);
    env.open(CABINET.c_str(), MDB_NOSUBDIR, 0600);

    // Fetch key/value pairs in a read-only transaction.
    auto rotxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
    auto dbiAll = lmdb::dbi::open(rotxn, "all");
    dbiAll.set_compare(rotxn, &compareKeys);
    std::clog << "Found " << dbiAll.size(rotxn) << " entries in database 'all'." << std::endl;

    auto dbi = lmdb::dbi::open(rotxn, DB.c_str());
    dbi.set_compare(rotxn, &compareKeys);
    const uint64_t totalEntries = dbi.size(rotxn);
    std::clog << "Found " << totalEntries << " entries in database '" << DB << "'." << std::endl;

    auto cursor = lmdb::cursor::open(rotxn, dbi);

    MDB_val key;
    MDB_val value;
    int32_t oldPercentage{-1};
    uint64_t entries{0};
    while (cursor.get(&key, &value, MDB_NEXT)) {
      entries++;

      MDB_val keyAll = key;
      MDB_val valueAll = value;

      // if we dump another table than "all", we need to look up the actual values from the original "all" table first.
      if (DB != "all") {
        keyAll = key;

        if (!lmdb::dbi_get(rotxn, dbiAll, &keyAll, &valueAll)) {
          continue;
        }
      }

      const char *ptr = static_cast<char*>(keyAll.mv_data);
      cabinet::Key storedKey = getKey(ptr, keyAll.mv_size);

      std::vector<char> val;
      val.reserve(storedKey.length());
      if (storedKey.length() > valueAll.mv_size) {
        LZ4_decompress_safe(static_cast<char*>(valueAll.mv_data), val.data(), valueAll.mv_size, val.capacity());
      }
      else {
        // Stored value is uncompressed.
        memcpy(val.data(), static_cast<char*>(valueAll.mv_data), valueAll.mv_size);
      }
      std::cout.write(static_cast<char*>(val.data()), storedKey.length());

/* 
			// Extract an Envelope and its payload on the example for AccelerationReading
      std::stringstream sstr{std::string(val.data(), storedKey.length())};
      auto e = cluon::extractEnvelope(sstr);
      if (e.first && e.second.dataType() == opendlv::proxy::AccelerationReading::ID()) {
        const auto tmp = cluon::extractMessage<opendlv::proxy::AccelerationReading>(std::move(e.second));
        std::cerr << tmp.accelerationX() << ", " << tmp.accelerationY() << std::endl;
      }
*/

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
