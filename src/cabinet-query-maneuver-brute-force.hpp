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

inline int64_t recursiveManeuverDetector(const int64_t _ts, const int _dsID, std::vector<DrivingStatus*> _maneuver, MDB_env* env, MDB_txn* rotxn, const bool &APLX) {

  if(_dsID >= (_maneuver.size())) {
    return _ts;
  }

  int64_t _currTS = _ts - _maneuver[_dsID]->minGap;
  int64_t _maxGapTS = _ts + _maneuver[_dsID]->maxGap;
  int64_t _maxTS = _ts + _maneuver[_dsID]->maxGap + _maneuver[_dsID]->maxDuration;

  uint16_t _ID = APLX ? opendlv::device::gps::pos::Grp1Data::ID() : opendlv::proxy::AccelerationReading::ID();


  //std::cout << "Checkpoint1" << std::endl;

  // Fetch key/value pairs in a read-only transaction.
  //auto rotxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
  auto dbiAll = lmdb::dbi::open(rotxn, "all");
  dbiAll.set_compare(rotxn, &compareKeys);
  //std::clog << "Found " << dbiAll.size(rotxn) << " entries in database 'all'." << std::endl;


  std::string DB = APLX ? "533/0" : "1030/2";
  auto dbi = lmdb::dbi::open(rotxn, DB.c_str());
  dbi.set_compare(rotxn, &compareKeys);
  const uint64_t totalEntries = dbi.size(rotxn);
  //std::clog << "Found " << totalEntries << " entries in database '" << DB << "'." << std::endl;

  //MDB_cursor *cursor;
  //mdb_cursor_open(rotxn, dbi, &cursor);
  auto cursor = lmdb::cursor::open(rotxn, dbi);

  bool flag = false;
  int64_t start_TS = 0;
  int64_t lastInFence_TS = 0;
  float lastInFence_accelLon = 0.0;
  float lastInFence_accelLat = 0.0;
  
  MDB_val key;
  MDB_val value;

  //while (mdb_cursor_get(cursor, &key, &value, MDB_NEXT_NODUP) == 0) {
  int32_t ent = 0;

  while (cursor.get(&key, &value, MDB_NEXT)) {
    ent++;
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

    // make sure, we are in range
    
    if(storedKey.timeStamp() < _currTS) continue;
    if(storedKey.timeStamp() > _maxTS) break; // break;
    
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

      float _currAccelLon = 0;
      float _currAccelTrans = 0;
      if(APLX) {
        const auto tmp = cluon::extractMessage<opendlv::device::gps::pos::Grp1Data>(std::move(e.second));
        _currAccelLon = tmp.accel_lon();
        _currAccelTrans = tmp.accel_trans();
      }
      else {
        const auto tmp = cluon::extractMessage<opendlv::proxy::AccelerationReading>(std::move(e.second));
        _currAccelLon = tmp.accelerationX();
        _currAccelTrans = tmp.accelerationY();
      }

      _currAccelLon = std::lroundf(_currAccelLon * 100.0f) / 100.0f;
      _currAccelTrans = std::lroundf(_currAccelTrans * 100.0f) / 100.0f;

      if(in_fence(_maneuver[_dsID]->fenceBL, _maneuver[_dsID]->fenceTR, _currAccelLon, _currAccelTrans) == true) {

        lastInFence_TS = storedKey.timeStamp();
        lastInFence_accelLon = _currAccelLon;
        lastInFence_accelLat = _currAccelTrans;
        
        if(flag == false) {
          //if(storedKey.timeStamp() > _maxGapTS) break;

          start_TS = storedKey.timeStamp();
          flag = true;
          continue;
        }
      }
      else {

        if(flag == false) continue;

        int64_t diffToPrev = storedKey.timeStamp() - lastInFence_TS;
        
        if(_maneuver[_dsID]->minDiffTime < diffToPrev) {
          int64_t duration = lastInFence_TS - start_TS;

          if((duration > _maneuver[_dsID]->minDuration) && (duration < _maneuver[_dsID]->maxDuration)) {
            // hier den nächsten Detektor

            cursor.close();
            return recursiveManeuverDetector(lastInFence_TS, _dsID+1, _maneuver, env, rotxn, APLX);
          }
          else {
            flag = false; // hier gibt es noch den Fall, dass immer noch das gleiche Manövr detektiert wird und der zweite Teil lang genug ist
          }
        }
      }
    }
  }
  cursor.close();
  //mdb_cursor_close(cursor);
  //rotxn.abort();

  return 0;
}


inline bool cabinet_queryManeuverBruteForce(const uint64_t &MEM, const std::string &CABINET, const std::string &MORTONCABINET, const bool &APLX, const bool &VERBOSE, const std::pair<float,float> _fenceBL, const std::pair<float,float> _fenceTR) {
  bool failed{false};
  try {

    std::pair<float,float> _fenceBL;
    std::pair<float,float> _fenceTR;

      ////////////////////////////////////////////////////////////////////////////////////////////////////////////
    _fenceBL.first = -1.5; _fenceBL.second = 0.75;
    _fenceTR.first = 0.75; _fenceTR.second = 5;
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

    _fenceBL.first = 4; _fenceBL.second = -4;
    _fenceTR.first = 10; _fenceTR.second = 4;
    DrivingStatus *harsh_braking = new DrivingStatus( "harsh_braking",
            _fenceBL,
            _fenceTR,
            500000000,
            3000000000,
            -200000000,
            2000000000,
            160000000);

    std::vector<DrivingStatus*> maneuver;
    
    maneuver.push_back(leftCurve);
    //maneuver.push_back(rightCurve);
    //maneuver.push_back(harsh_braking);

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

    //MDB_cursor *cursor;
    //mdb_cursor_open(rotxn, dbi, &cursor);
    auto cursor = lmdb::cursor::open(rotxn, dbi);

    bool flag = false;
    int64_t start_TS = 0;
    int64_t lastInFence_TS = 0;
    float lastInFence_accelLon = 0.0;
    float lastInFence_accelLat = 0.0;
        
    std::vector<std::pair<int64_t, int64_t>> maneuverDetectedList;

    int32_t oldPercentage{-1};
    uint64_t entries{0};

    MDB_val key;
    MDB_val value;

    //while (mdb_cursor_get(cursor, &key, &value, MDB_NEXT_NODUP) == 0) {

    while (cursor.get(&key, &value, MDB_NEXT)) {

      const int32_t percentage = static_cast<int32_t>((static_cast<float>(entries) * 100.0f) / static_cast<float>(totalEntries));
      if ((percentage % 5 == 0) && (percentage != oldPercentage)) {
        std::clog <<"Processed " << percentage << "% (" << entries << " entries) from " << CABINET << std::endl;
        oldPercentage = percentage;
      }
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
        //const auto tmp = cluon::extractMessage<opendlv::device::gps::pos::Grp1Data>(std::move(e.second));

        float _currAccelLon = 0;
        float _currAccelTrans = 0;
        if(APLX) {
          const auto tmp = cluon::extractMessage<opendlv::device::gps::pos::Grp1Data>(std::move(e.second));
          _currAccelLon = tmp.accel_lon();
          _currAccelTrans = tmp.accel_trans();
        }
        else {
          const auto tmp = cluon::extractMessage<opendlv::proxy::AccelerationReading>(std::move(e.second));
          _currAccelLon = tmp.accelerationX();
          _currAccelTrans = tmp.accelerationY();
        }

        _currAccelLon = std::lroundf(_currAccelLon * 100.0f) / 100.0f;
        _currAccelTrans = std::lroundf(_currAccelTrans * 100.0f) / 100.0f;

        if (VERBOSE) std::cerr << _currAccelLon << ", " << _currAccelTrans << "  : " << storedKey.timeStamp() << std::endl;

        if(in_fence(maneuver[0]->fenceBL, maneuver[0]->fenceTR, _currAccelLon, _currAccelTrans) == true) {
          lastInFence_TS = storedKey.timeStamp();
          lastInFence_accelLon = _currAccelLon;
          lastInFence_accelLat = _currAccelTrans;
          
          if(flag == false) {
            start_TS = storedKey.timeStamp();
            flag = true;
          }
        }
        else {

          if(flag == false) continue;

          int64_t diffToPrev = storedKey.timeStamp() - lastInFence_TS;
          
          if(maneuver[0]->minDiffTime < diffToPrev) {
            int64_t duration = lastInFence_TS - start_TS;

            if((duration > maneuver[0]->minDuration) && (duration < maneuver[0]->maxDuration)) {
              // hier den nächsten Detektor
              int64_t end_TS = 0; // detector aufruf
              
              end_TS = recursiveManeuverDetector(lastInFence_TS, 1, maneuver, env, rotxn, APLX);
              
              if(end_TS != 0){
                //std::cout << "Maneuver" << start_TS << ", " << end_TS << std::endl;
                // maneuver speichern (start, end)
                maneuverDetectedList.push_back(std::make_pair(start_TS, end_TS));
              }
              flag = false;
            }
            else {
              flag = false; // hier gibt es noch den Fall, dass immer noch das gleiche Manövr detektiert wird und der zweite Teil lang genug ist
            }
          }
        }

      }
    }

    for(auto _temp : maneuverDetectedList) {
      std::cout << "Maneuver detected at: " << _temp.first << ", " << _temp.second << std::endl;
    }
    std::cout << "We detected " << maneuverDetectedList.size() << " Maneuververs" << std::endl;

    cursor.close();
    //mdb_cursor_close(cursor);
    rotxn.abort();
  }

  catch (...) {
    failed = true;
  }
  return failed;
}

#endif