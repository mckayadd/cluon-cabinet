/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef CABINETACCELTOMORTON_HPP
#define CABINETACCELTOMORTON_HPP

#include "cluon-complete.hpp"
#include "opendlv-standard-message-set.hpp"
#include "key.hpp"
#include "morton.hpp"
#include "lmdb++.h"
#include "lz4.h"

#include <iostream>
#include <sstream>
#include <string>

inline bool in_fence(const std::pair<float,float> _fenceBL, const std::pair<float,float> _fenceTR, const float _x, const float _y) {
  if((_x >= _fenceBL.first) && (_x <= _fenceTR.first)){
    if((_y >= _fenceBL.second) && (_y <= _fenceTR.second)){
      return true;
    }
  }
  return false;
}

inline bool cabinet_queryManeuverBruteForce(const uint64_t &MEM, const std::string &CABINET, const std::string &MORTONCABINET, const bool &APLX, const bool &VERBOSE, const std::pair<float,float> _fenceBL, const std::pair<float,float> _fenceTR) {
  bool failed{false};
  try {

    uint16_t _ID = APLX ? opendlv::device::gps::pos::Grp1Data::ID() : opendlv::proxy::AccelerationReading::ID();
    uint16_t _subID = APLX ? 0 : 2;
    //uint16_t _ID = opendlv::proxy::AccelerationReading::ID();
    auto env = lmdb::env::create();
    env.set_mapsize(MEM/2 * 1024UL * 1024UL * 1024UL);
    env.set_max_dbs(100);
    env.open(CABINET.c_str(), MDB_NOSUBDIR, 0600);

    // Fetch key/value pairs in a read-only transaction.
    auto rotxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
    auto dbiAll = lmdb::dbi::open(rotxn, "all");
    dbiAll.set_compare(rotxn, &compareKeys);
    std::cerr << "Found " << dbiAll.size(rotxn) << " entries." << std::endl;

    auto dbi = APLX ? lmdb::dbi::open(rotxn, "533/0") : lmdb::dbi::open(rotxn, "1030/2");
    //auto dbi = lmdb::dbi::open(rotxn, "1030/2");
    dbi.set_compare(rotxn, &compareKeys);
    const uint64_t totalEntries = dbi.size(rotxn);
    std::cerr << "Found " << totalEntries << " entries." << std::endl;

    auto cursor = lmdb::cursor::open(rotxn, dbi);

    bool flag = false;
    int64_t start_TS = 0;

    MDB_val key;
    MDB_val value;
    int32_t oldPercentage{-1};
    uint64_t entries{0};
    while (cursor.get(&key, &value, MDB_NEXT)) {
      entries++;
      MDB_val keyAll;
      MDB_val valueAll;
      keyAll = key;
      const char *ptr2 = static_cast<char*>(key.mv_data);
      cabinet::Key storedKey2 = getKey(ptr2, key.mv_size);

      if (lmdb::dbi_get(rotxn, dbiAll, &keyAll, &valueAll)) {
        const char *ptr = static_cast<char*>(keyAll.mv_data);
        cabinet::Key storedKey = getKey(ptr, keyAll.mv_size);
        if (storedKey.dataType() == _ID) {
          std::vector<char> val;
          val.reserve(storedKey.length());
          if (storedKey.length() > valueAll.mv_size) {
              LZ4_decompress_safe(static_cast<char*>(valueAll.mv_data), val.data(), valueAll.mv_size, val.capacity());
          }
          else {
            // Stored value is uncompressed.
            // recFile.write(static_cast<char*>(val.mv_data), val.mv_size);
            memcpy(val.data(), static_cast<char*>(valueAll.mv_data), valueAll.mv_size);
          }
          std::stringstream sstr{std::string(val.data(), storedKey.length())};
          auto e = cluon::extractEnvelope(sstr);
          if (e.first && e.second.senderStamp() == _subID) {
            // Compose name for database.
            //std::stringstream _dataType_senderStamp;
            //_dataType_senderStamp << _ID << '/'<< e.second.senderStamp() << "-morton";
            //const std::string _shortKey{_dataType_senderStamp.str()};

            // Extract value from Envelope and compute Morton code.
            if(APLX) {
              const auto tmp = cluon::extractMessage<opendlv::device::gps::pos::Grp1Data>(std::move(e.second));
              auto morton = convertAccelLonTransToMorton(std::make_pair(tmp.accel_lon(), tmp.accel_trans()));
              if (VERBOSE) {
                std::cerr << tmp.accel_lon() << ", " << tmp.accel_trans() << " = " << morton << ", " << storedKey.timeStamp() << std::endl;
              }
              
              if((flag == false) && (in_fence(_fenceBL, _fenceTR, tmp.accel_lon(), tmp.accel_trans()) == true)) {
                start_TS = storedKey.timeStamp();
                flag = true;
                std::cerr << "In fence" << std::endl;
              }
              if((flag == true) && (in_fence(_fenceBL, _fenceTR, tmp.accel_lon(), tmp.accel_trans()) == false)) {
                
                int64_t duration = storedKey.timeStamp() - start_TS;

                if(duration) // TODO:
              }
            }
            else
            {
              // TODO:
            }
          }
        }
      }
    }

    
        


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////777

/* 
    uint16_t _ID = APLX ? opendlv::device::gps::pos::Grp1Data::ID() : opendlv::proxy::AccelerationReading::ID();
    uint16_t _subID = APLX ? 0 : 2;
    //uint16_t _ID = opendlv::proxy::AccelerationReading::ID();
    auto env = lmdb::env::create();
    env.set_mapsize(MEM/2 * 1024UL * 1024UL * 1024UL);
    env.set_max_dbs(100);
    env.open(CABINET.c_str(), MDB_NOSUBDIR, 0600);

    // Fetch key/value pairs in a read-only transaction.
    auto rotxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
    auto dbiAll = lmdb::dbi::open(rotxn, "all");
    dbiAll.set_compare(rotxn, &compareKeys);
    std::cerr << "Found " << dbiAll.size(rotxn) << " entries." << std::endl;

    auto dbi = APLX ? lmdb::dbi::open(rotxn, "533/0") : lmdb::dbi::open(rotxn, "1030/2");
    //auto dbi = lmdb::dbi::open(rotxn, "1030/2");
    dbi.set_compare(rotxn, &compareKeys);
    const uint64_t totalEntries = dbi.size(rotxn);
    std::cerr << "Found " << totalEntries << " entries." << std::endl;

    auto cursor = lmdb::cursor::open(rotxn, dbi);

    MDB_val key;
    MDB_val value;
    int32_t oldPercentage{-1};
    uint64_t entries{0};
    int _rc = 0;
    bool flag = false;

    while (cursor.get(&key, &value, MDB_NEXT)) {
      //entries++;
      MDB_val keyAll;
      MDB_val valueAll;
      keyAll = key;
      const char *ptr2 = static_cast<char*>(key.mv_data);
      cabinet::Key storedKey2 = getKey(ptr2, key.mv_size);

      if (lmdb::dbi_get(rotxn, dbiAll, &keyAll, &valueAll)) {
        const char *ptr = static_cast<char*>(keyAll.mv_data);
        cabinet::Key storedKey = getKey(ptr, keyAll.mv_size);
        if (storedKey.dataType() == _ID) {
          std::vector<char> val;
          val.reserve(storedKey.length());
          if (storedKey.length() > valueAll.mv_size) {
              LZ4_decompress_safe(static_cast<char*>(valueAll.mv_data), val.data(), valueAll.mv_size, val.capacity());
          }
          else {
            // Stored value is uncompressed.
            // recFile.write(static_cast<char*>(val.mv_data), val.mv_size);
            memcpy(val.data(), static_cast<char*>(valueAll.mv_data), valueAll.mv_size);
          }
          std::stringstream sstr{std::string(val.data(), storedKey.length())};
          auto e = cluon::extractEnvelope(sstr);
          if (e.first && e.second.senderStamp() == _subID) {
            // Compose name for database.
            //std::stringstream _dataType_senderStamp;
            //_dataType_senderStamp << _ID << '/'<< e.second.senderStamp() << "-morton";
            //const std::string _shortKey{_dataType_senderStamp.str()};

            // Extract value from Envelope and compute Morton code.
            if(APLX) {
              const auto tmp = cluon::extractMessage<opendlv::device::gps::pos::Grp1Data>(std::move(e.second));

              //if(flag == false) { // TODO: und in fecne
                int64_t start_ts = storedKey.timeStamp();
                storedKey.timeStamp(start_ts + 10000000000);
                entries++;
                flag = true;
              //}


              // cursor weitersetzen
              //_rc = mdb_cursor_get(cursor, &key, &value, MDB_NEXT);

              auto morton = convertAccelLonTransToMorton(std::make_pair(tmp.accel_lon(), tmp.accel_trans()));

// here I know the content that is related to a time step, hier kann ich die Abfrage starten; ich muss noch herausfinden, wie ich den TS wieder zuruecksetzen kann
// das muesste recht einfach gehen, wenn ich den Key neu setze und continue; mache, dann springt der wieder in die while schleife, ich muss dann aber noch das mit dem nächsten Wert machen da
// und ich muss herausfinden, wie ich den Timestamp auch wirklich setze

            }
            else {
              // TODO:
            }


          }
        }
      }

            const int32_t percentage = static_cast<int32_t>((static_cast<float>(entries) * 100.0f) / static_cast<float>(totalEntries));
      if ((percentage % 5 == 0) && (percentage != oldPercentage)) {
        std::clog <<"Processed " << percentage << "% (" << entries << " entries) from " << CABINET << std::endl;
        oldPercentage = percentage;
      }
    } */

    std::clog << "number of entries " << entries << std::endl;
  

    cursor.close();
    rotxn.abort();
  }
  catch (...) {
    failed = true;
  }
  return failed;
}


inline void store_data() {

}

#endif
