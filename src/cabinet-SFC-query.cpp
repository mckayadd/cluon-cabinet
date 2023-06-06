/*
 * Copyright (C) 2023  Christian Berger, Lukas Birkemeyer
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"
#include "key.hpp"
#include "morton.hpp"
#include "lmdb++.h"

#include <cstdio>
#include <cstring>
#include <cstdint>

#include <iostream>
#include <string>

int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if ( (0 == commandlineArguments.count("cab")) ) {
    std::cerr << argv[0] << " query a cabinet (an lmdb-based key/value-database with accelerations in Morton format)." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --cab=myStore.cab [--mem=32024] --box=bottom-left-accelX,bottom-left-accelY,top-right-accelX,top-right-accelY" << std::endl;
    std::cerr << "         --cab:    name of the cabinet file" << std::endl;
    std::cerr << "         --db:     name of the database to be used inside the cabinet file; default: 1030/0-morton" << std::endl;
    std::cerr << "         --mem:    upper memory size for database in memory in GB, default: 64,000 (representing 64TB)" << std::endl;
    std::cerr << "         --box:    return all timeStamps within this rectangle specified by bottom-left and top-right X/Y accelerations" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cab=myStore.cab --box=0,-2,2,2   # random driving" << std::endl;
    std::cerr << "         " << argv[0] << " --cab=myStore.cab --box=4,-4,10,4  # harsh braking" << std::endl;
    retCode = 1;
  } else {    
    // TODO: Do a "geofence" to filter
    const std::string CABINET{commandlineArguments["cab"]};
    const std::string DB{(commandlineArguments["db"].size() != 0) ? commandlineArguments["db"] : "1030/0-morton"};
    const uint64_t MEM{(commandlineArguments["mem"].size() != 0) ? static_cast<uint64_t>(std::stoi(commandlineArguments["mem"])) : 64UL*1024UL};
    const bool VERBOSE{commandlineArguments["verbose"].size() != 0};
    const std::string BOX{commandlineArguments["box"]};

    std::vector<std::string> boxStrings = stringtoolbox::split(BOX, ',');
    if (4 == boxStrings.size()) {
      // Extract the bounding box and turn into floating point numbers.
      std::pair<float,float> boxBL;
      boxBL.first = std::stof(boxStrings.at(0));
      boxBL.second = std::stof(boxStrings.at(1));

      std::pair<float,float> boxTR;
      boxTR.first = std::stof(boxStrings.at(2));
      boxTR.second = std::stof(boxStrings.at(3));

      try {
        // Initialize lmdb environment (support up to 100 tables).
        auto env = lmdb::env::create();
        env.set_max_dbs(100);
        // Allocate enough virtual memory for the database.
        env.set_mapsize(MEM * 1024UL * 1024UL * 1024UL);
        // Open database.
        env.open(CABINET.c_str(), MDB_NOSUBDIR, 0600);

        // Fetch key/value pairs in a read-only transaction.
        auto rotxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
        auto dbi = lmdb::dbi::open(rotxn, DB.c_str());

        const uint64_t totalEntries{dbi.size(rotxn)};
        if (VERBOSE) {
          std::cerr << "Found " << totalEntries << " entries in db '" << DB << "'" << std::endl;
        }

        // Keys are sorted by the Morton index
        dbi.set_compare(rotxn, &compareMortonKeys);
        // ...but a single Morton index can hold several values that are sorted by time using the original sorting function.
        lmdb::dbi_set_dupsort(rotxn, dbi, &compareKeys);

        // Iterate over all entries.
        uint64_t entries{0};
        auto cursor = lmdb::cursor::open(rotxn, dbi);
        MDB_val key;
        MDB_val value;
        while (cursor.get(&key, &value, MDB_NEXT)) {
          entries++;
        }
        cursor.close();

        if (VERBOSE) {
          std::cerr << "Extracted " << entries << "/" << totalEntries << " from db '" << DB << "'" << std::endl;
        }


        // As we use the database in read-only mode, we simply abort the transaction.
        rotxn.abort();
        // Database will be closed automatically once this scope is left.
      } catch (const lmdb::error& error) {
        std::cerr << "Error while interfacing with the database: " << error.what()<< std::endl;
        retCode = 1;
      }
    }

    return retCode;
  }
}
