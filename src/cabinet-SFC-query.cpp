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
#include "geofence.hpp"
#include "lmdb++.h"

#include <cstdio>
#include <cstring>
#include <cstdint>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <string>

int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if ( (0 == commandlineArguments.count("cab")) ) {
    std::cerr << argv[0] << " query a cabinet (an lmdb-based key/value-database with accelerations in Morton format)." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --cab=myStore.cab [--mem=32024] --box=bottom-left-accelX,bottom-left-accelY,top-right-accelX,top-right-accelY" << std::endl;
    std::cerr << "         --cab:      name of the cabinet file" << std::endl;
    std::cerr << "         --db:       name of the database to be used inside the cabinet file; default: 1030/0-morton" << std::endl;
    std::cerr << "         --mem:      upper memory size for database in memory in GB, default: 64,000 (representing 64TB)" << std::endl;
    std::cerr << "         --box:      return all timeStamps within this rectangle specified by bottom-left and top-right X/Y accelerations; default: 0,-2,2,2" << std::endl;
    std::cerr << "         --start:    only include matching timeStamps that are larger than or equal to this timepoint in nanoseconds; default: 0" << std::endl;
    std::cerr << "         --end:      only include matching timeStamps that are less than this timepoint in nanoseconds; default: MAX" << std::endl;
    std::cerr << "         --print:    prints the matching timestamps" << std::endl;
    std::cerr << "         --verbose" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cab=myStore.cab --box=0,-2,2,2   # random driving" << std::endl;
    std::cerr << "         " << argv[0] << " --cab=myStore.cab --box=4,-4,10,4  # harsh braking" << std::endl;
    retCode = 1;
  } else {    
    const std::string CABINET{commandlineArguments["cab"]};
    const std::string DB{(commandlineArguments["db"].size() != 0) ? commandlineArguments["db"] : "1030/0-morton"};
    const uint64_t MEM{(commandlineArguments["mem"].size() != 0) ? static_cast<uint64_t>(std::stoi(commandlineArguments["mem"])) : 64UL*1024UL};
    const std::string BOX{(commandlineArguments["box"].size() != 0) ? commandlineArguments["box"] : "0,-2,2,2"}; // random driving maneuver
    const uint64_t START{(commandlineArguments.count("start") != 0) ? static_cast<uint64_t>(std::stoll(commandlineArguments["start"])) : 0};
    const uint64_t END{(commandlineArguments.count("end") != 0) ? static_cast<uint64_t>(std::stoll(commandlineArguments["end"])) : std::numeric_limits<uint64_t>::max()};
    const bool PRINT{commandlineArguments["print"].size() != 0};
    const bool VERBOSE{commandlineArguments["verbose"].size() != 0};

    std::vector<std::string> boxStrings = stringtoolbox::split(BOX, ',');
    if (4 == boxStrings.size()) {
      // Extract the bounding box and turn into floating point numbers.
      std::pair<float,float> boxBL;
      boxBL.first = std::stof(boxStrings.at(0));
      boxBL.second = std::stof(boxStrings.at(1));
      const uint64_t bl_morton{convertAccelLonTransToMorton(boxBL)};

      std::pair<float,float> boxTR;
      boxTR.first = std::stof(boxStrings.at(2));
      boxTR.second = std::stof(boxStrings.at(3));
      const uint64_t tr_morton{convertAccelLonTransToMorton(boxTR)};

      // Prepare polygon to test whether the identified de-Morton-ized values actually reside inside the specified box.
      std::pair<float,float> boxTL;
      std::pair<float,float> boxBR;
      boxTL.first = std::stof(boxStrings.at(2));
      boxTL.second = std::stof(boxStrings.at(1));
      boxBR.first = std::stof(boxStrings.at(0));
      boxBR.second = std::stof(boxStrings.at(3));

      std::vector<std::array<double,2>> polygon;
      std::array<double,2> a{boxBL.first, boxBL.second};
      std::array<double,2> b{boxBR.first, boxBR.second};
      std::array<double,2> c{boxTR.first, boxTR.second};
      std::array<double,2> d{boxTL.first, boxTL.second};
      polygon.push_back(a);
      polygon.push_back(b);
      polygon.push_back(c);
      polygon.push_back(d);

      if (VERBOSE) {
        std::clog << "[" << argv[0] << "]: (" << boxBL.first << "," << boxBL.second << ") = " <<  bl_morton << " (" << boxTR.first << "," << boxTR.second << ") = " << tr_morton << std::endl;
      }

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
          std::cerr << "[" << argv[0] << "] Found " << totalEntries << " entries in db '" << DB << "'" << std::endl;
        }

        // Keys are sorted by the Morton index
        dbi.set_compare(rotxn, &compareMortonKeys);
        // ...but a single Morton index can hold several values that are sorted by time using the original sorting function.
        lmdb::dbi_set_dupsort(rotxn, dbi, &compareKeys);

        // We use a cursor to traverse the database.
        auto cursor = lmdb::cursor::open(rotxn, dbi);

        uint64_t entries{0};
        MDB_val key;
        MDB_val value;

        // We want to use the range query, ie., finding all keys that are equal or larger than this specified, Morton-ized key.
        key.mv_size = sizeof(bl_morton);
        auto _bl_morton = htobe64(bl_morton);
        key.mv_data = &_bl_morton;
        try {
          if (cursor.get(&key, &value, MDB_SET_RANGE)) {
            std::vector<uint64_t> listOfTimeStamps;
            while (cursor.get(&key, &value, MDB_NEXT)) {
              // Get next Morton-ized value.
              uint64_t morton = *reinterpret_cast<uint64_t*>(key.mv_data);
              morton = be64toh(morton);

              // Stop iterating once we have reached TL corner of the box.
              if (morton > tr_morton) break;

              // If this Morton value is within the single dimensional range, decode it back to the actual X/Y acceleration.
              auto decodedAccelLonTrans = convertMortonToAccelLonTrans(morton);

              // Test whether the de-Morton-ized X/Y acceleration is actually within the specified box.
              std::array<double,2> pos{decodedAccelLonTrans.first, decodedAccelLonTrans.second};
              if (geofence::isIn<double>(polygon, pos)) {
                // The Morton-ized keys point to a list of timeStamps when the vehicle had this X/Y acceleration; hence, we get the associated value(s) to this Morton index.
                if (value.mv_size == sizeof(int64_t)) {
                  const char *ptr = static_cast<char*>(value.mv_data);
                  int64_t timeStamp{0};
                  std::memcpy(&timeStamp, ptr, value.mv_size);
                  timeStamp = be64toh(timeStamp);

                  if (VERBOSE) {
                    std::cerr << "[" << argv[0] << "] " << bl_morton << ";" << morton << ";" << tr_morton << ": " 
                              << std::setprecision(4) << decodedAccelLonTrans.first << "," << decodedAccelLonTrans.second << "@" << timeStamp << std::endl;
                  }
                  uint64_t _timeStamp = static_cast<uint64_t>(timeStamp);
                  listOfTimeStamps.push_back(_timeStamp);
                }
                // no geofence test:   Extracted 237607/239923 from db '1030/2-morton' using the FakeEuroFOT database.
                // with geofence test: Extracted 153162/239923 from db '1030/2-morton' using the FakeEuroFOT database.
              }
            }
            std::sort(listOfTimeStamps.begin(), listOfTimeStamps.end());
            if (PRINT) {
              for (auto i : listOfTimeStamps) {
                if ( (i >= START) && (i < END) ) {
                  std::cout << i << ",";
                  entries++;
                }
              }
              std::cout << std::endl;
            }
          }
        } catch (const lmdb::error& error) {
          std::cerr << "[" << argv[0] << "] Error while interfacing with the database: " << error.what() << std::endl;
          retCode = 1;
        }
        cursor.close();

        if (VERBOSE) {
          std::cerr << "[" << argv[0] << "] Extracted " << entries << "/" << totalEntries << " from db '" << DB << "'" << std::endl;
        }

        // As we use the database in read-only mode, we simply abort the transaction.
        rotxn.abort();
        // Database will be closed automatically once this scope is left.
      } catch (const lmdb::error& error) {
        std::cerr << "[" << argv[0] << "] Error while interfacing with the database: " << error.what() << std::endl;
        retCode = 1;
      }
    }

    return retCode;
  }
}
