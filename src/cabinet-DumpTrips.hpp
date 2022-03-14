/*
 * Copyright (C) 2022  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef CABINETDUMPTRIPS_HPP
#define CABINETDUMPTRIPS_HPP

#include "cluon-complete.hpp"
#include "opendlv-standard-message-set.hpp"
#include "key.hpp"
#include "lmdb++.h"

#include <iostream>
#include <sstream>
#include <string>

inline bool cabinet_DumpTrips(const uint64_t &MEM, const std::string &CABINET) {
  bool failed{false};
  try {
    auto env = lmdb::env::create();
    env.set_mapsize(MEM/2 * 1024UL * 1024UL * 1024UL);
    env.set_max_dbs(100);
    env.open(CABINET.c_str(), MDB_NOSUBDIR, 0600);

    // Fetch key/value pairs in a read-only transaction.
    auto rotxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
    auto dbi = lmdb::dbi::open(rotxn, "trips");
    dbi.set_compare(rotxn, &compareKeys);
    const uint64_t totalEntries = dbi.size(rotxn);
    std::cerr << "Found " << totalEntries << " entries in db 'trips'" << std::endl;
    auto cursor = lmdb::cursor::open(rotxn, dbi);
    MDB_val key;
    MDB_val value;
    int32_t oldPercentage{-1};
    uint64_t entries{0};
    while (cursor.get(&key, &value, MDB_NEXT)) {
      entries++;
      const char *ptrKey = static_cast<char*>(key.mv_data);
      cabinet::Key storedKey = getKey(ptrKey, key.mv_size);

      const char *ptrValue = static_cast<char*>(value.mv_data);
      cabinet::Key storedValue = getKey(ptrValue, value.mv_size);

      std::cout << storedKey.timeStamp() << ";" << storedValue.timeStamp() << std::endl;

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
