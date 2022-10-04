/*
 * Copyright (C) 2022  Lukas Birkemeyer
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
#include "DrivingStatus.hpp"

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

inline int64_t recursiveManeuverDetector(const int64_t _ts, const int _dsID, std::vector<DrivingStatus*> _maneuver) {

  if(_dsID >= _maneuver.size())
    return _ts;

  int64_t _currTS = _ts - _maneuver[_dsID]->minGap;
  int64_t _maxTS = _ts - _maneuver[_dsID]->maxGap;

  return 0;
}


inline int64_t rManeuverDetector() {

  return 0;
}

inline bool cabinet_queryManeuverBruteForce(const uint64_t &MEM, const std::string &CABINET, const std::string &MORTONCABINET, const bool &APLX, const bool &VERBOSE, const std::pair<float,float> _fenceBL, const std::pair<float,float> _fenceTR) {
  bool failed{false};
  try {

    std::pair<float,float> _fenceBL;
    std::pair<float,float> _fenceTR;

      ////////////////////////////////////////////////////////////////////////////////////////////////////////////
      //_fenceBL.first = -1.5; _fenceBL.second = 0.75;
      //_fenceTR.first = 0.75; _fenceTR.second = 5;
      _fenceBL.first = 2; _fenceBL.second = -1;
      _fenceTR.first = 8; _fenceTR.second = 1;
      DrivingStatus *leftCurve  = new DrivingStatus( "leftCurve",
              _fenceBL,
              _fenceTR,
              500000000,
              3000000000,
              -200000000,
              2000000000,
              160000000);

      _fenceBL.first = -1.5; _fenceBL.second = -5;
      _fenceTR.first = 0.75; _fenceTR.second = -0.75;
      DrivingStatus *rightCurve = new DrivingStatus( "rightCurve",
              _fenceBL,
              _fenceTR,
              500000000,
              3000000000,
              -200000000,
              2000000000,
              160000000);

      std::vector<DrivingStatus*> maneuver;
      
      maneuver.push_back(leftCurve);
      maneuver.push_back(rightCurve);

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////

    uint16_t _ID = APLX ? opendlv::device::gps::pos::Grp1Data::ID() : opendlv::proxy::AccelerationReading::ID();

    auto env = lmdb::env::create();
    env.set_mapsize(MEM * 1024UL * 1024UL * 1024UL);
    env.set_max_dbs(100);
    env.open(CABINET.c_str(), MDB_NOSUBDIR, 0600);

    // Fetch key/value pairs in a read-only transaction.
    auto rotxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
    auto dbiAll = lmdb::dbi::open(rotxn, "all");
    dbiAll.set_compare(rotxn, &compareKeys);
    std::clog << "Found " << dbiAll.size(rotxn) << " entries in database 'all'." << std::endl;


    std::string DB = APLX ? "533/0" : "1030/2";
    auto dbi = lmdb::dbi::open(rotxn, DB.c_str());
    dbi.set_compare(rotxn, &compareKeys);
    const uint64_t totalEntries = dbi.size(rotxn);
    std::clog << "Found " << totalEntries << " entries in database '" << DB << "'." << std::endl;

    auto cursor = lmdb::cursor::open(rotxn, dbi);
    
    bool flag = false;
    int64_t start_TS = 0;
    int64_t lastInFence_TS = 0;
    float lastInFence_accelLon = 0.0;
    float lastInFence_accelLat = 0.0;
    
    std::vector<std::pair<int64_t, int64_t>> maneuverDetectedList;


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
      //std::cout.write(static_cast<char*>(val.data()), storedKey.length());


      // Extract an Envelope and its payload on the example for AccelerationReading
      std::stringstream sstr{std::string(val.data(), storedKey.length())};
      auto e = cluon::extractEnvelope(sstr);
      if (e.first && e.second.dataType() == _ID) {
        // const auto tmp = cluon::extractMessage<opendlv::proxy::AccelerationReading>(std::move(e.second));
        const auto tmp = cluon::extractMessage<opendlv::device::gps::pos::Grp1Data>(std::move(e.second));

        if (VERBOSE) std::cerr << tmp.accel_lon() << ", " << tmp.accel_trans() << std::endl;

                                  
        if(in_fence(_fenceBL, _fenceTR, tmp.accel_lon(), tmp.accel_trans()) == true) {
          lastInFence_TS = storedKey.timeStamp();
          lastInFence_accelLon = tmp.accel_lon();
          lastInFence_accelLat = tmp.accel_trans();
          
          if(flag == false) {
            start_TS = storedKey.timeStamp();
            flag = true;
            continue;
          }
        }
        else {
          int64_t diffToPrev = storedKey.timeStamp() - lastInFence_TS;
          
          if(diffToPrev > maneuver[0]->minDiffTime) {
            int64_t duration = lastInFence_TS - start_TS;

            if(duration > maneuver[0]->maxDuration) { // out of range
              flag = false; // hier gibt es noch den Fall, dass immer noch das gleiche Manövr detektiert wird und der zweite Teil lang genug ist
              continue;
            }
            if((duration >= maneuver[0]->minDuration) && (duration <= maneuver[0]->maxDuration)) {
              // hier den nächsten Detektor
              int64_t end_TS = 0; // detector aufruf

              // end_TS = recursiveManeuverDetector(lastInFence_TS, 1, maneuver);

              if(end_TS != 0){
                // maneuver speichern (start, end)
                maneuverDetectedList.push_back(std::make_pair(start_TS, end_TS));
              }
              flag = false;
            }
          }
        }

        const int32_t percentage = static_cast<int32_t>((static_cast<float>(entries) * 100.0f) / static_cast<float>(totalEntries));
        if ((percentage % 5 == 0) && (percentage != oldPercentage)) {
          std::clog <<"Processed " << percentage << "% (" << entries << " entries) from " << CABINET << std::endl;
          oldPercentage = percentage;
        }
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


    /* uint16_t _ID = APLX ? opendlv::device::gps::pos::Grp1Data::ID() : opendlv::proxy::AccelerationReading::ID();
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
    int64_t lastInFence_TS = 0;
    float lastInFence_accelLon = 0.0;
    float lastInFence_accelLat = 0.0;

    int32_t oldPercentage{-1};
    uint64_t entries{0};
    std::vector<std::pair<int64_t, int64_t>> maneuverDetectedList;

    MDB_val key;
    MDB_val value;

    while (mdb_cursor_get(cursor, &key, &value, MDB_NEXT_NODUP) == 0) { //cursor.get(&key, &value, MDB_NEXT)
      entries++;
      MDB_val keyAll;
      MDB_val valueAll;
      keyAll = key;
      const char *ptr2 = static_cast<char*>(key.mv_data);
      cabinet::Key storedKey2 = getKey(ptr2, key.mv_size);

      const uint64_t MAXKEYSIZE = 511;
      std::vector<char> _key;
      _key.reserve(MAXKEYSIZE);

      storedKey2.timeStamp(1646666992460496002);

      key.mv_size = setKey(storedKey2, _key.data(), _key.capacity());
      key.mv_data = _key.data();

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

                           
              if(in_fence(_fenceBL, _fenceTR, tmp.accel_lon(), tmp.accel_trans()) == true) {
                lastInFence_TS = storedKey.timeStamp();
                lastInFence_accelLon = tmp.accel_lon();
                lastInFence_accelLat = tmp.accel_trans();
                
                if(flag == false) {
                  start_TS = storedKey.timeStamp();
                  flag = true;
                  continue;
                }
              }
              else {
                int64_t diffToPrev = storedKey.timeStamp() - lastInFence_TS;
                
                if(diffToPrev > maneuver[0]->minDiffTime) {
                  int64_t duration = lastInFence_TS - start_TS;

                  if(duration > maneuver[0]->maxDuration) { // out of range
                    flag = false; // hier gibt es noch den Fall, dass immer noch das gleiche Manövr detektiert wird und der zweite Teil lang genug ist
                    continue;
                  }
                  if((duration >= maneuver[0]->minDuration) && (duration <= maneuver[0]->maxDuration)) {
                    // hier den nächsten Detektor
                    int64_t end_TS = 0; // detector aufruf

                    // end_TS = recursiveManeuverDetector(lastInFence_TS, 1, maneuver);

                    if(end_TS != 0){
                      // maneuver speichern (start, end)
                      maneuverDetectedList.push_back(std::make_pair(start_TS, end_TS));
                    }
                    flag = false;
                  }
                }
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

    for(auto _temp : maneuverDetectedList) {
      std::cout << "Maneuver detected at: " << _temp.first << ", " << _temp.second << std::endl;
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
/*
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

} */
