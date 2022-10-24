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

// namespace brute_force {

  inline bool in_fence(const std::pair<float,float> _fenceBL, const std::pair<float,float> _fenceTR, const float _x, const float _y) {
    if((_x >= _fenceBL.first) && (_x <= _fenceTR.first)){
      if((_y >= _fenceBL.second) && (_y <= _fenceTR.second)){
        return true;
      }
    }
    return false;
  }

  inline int64_t recursiveManeuverDetector(const int64_t _ts, const int _dsID, std::vector<DrivingStatus*> _maneuver, MDB_env* env, MDB_txn* rotxn, const bool &APLX, const bool &VERBOSE) {

    if(_dsID >= (_maneuver.size())) {
      return _ts;
    }

    //if((_ts >=  1645101196015660000) && (_ts <=  1645101199996113000))
    //  std::cout << "start" << std::endl;

    int64_t _currTS = _ts + _maneuver[_dsID]->minGap;
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
    int64_t prev_TS = 0;
    float lastInFence_accelLon = 0.0;
    float lastInFence_accelLat = 0.0;
    
    MDB_val key;
    MDB_val value;

    const uint64_t MAXKEYSIZE = 511;
    std::vector<char> _key;
    _key.reserve(MAXKEYSIZE);

    cabinet::Key query;
    query.timeStamp(_currTS - _maneuver[_dsID]->minDiffTime); // um den außreiser bereich noch zurück gehen, um sicher zu stelle, dass manöver nicht schon vor gap min anfängt
    
    key.mv_size = setKey(query, _key.data(), _key.capacity());
    key.mv_data = _key.data();

    int32_t retCode{0};
    // lambda to check the interaction with the database.
  
    mdb_cursor_get(cursor, &key, &value, MDB_SET_RANGE);

    //while (mdb_cursor_get(cursor, &key, &value, MDB_NEXT_NODUP) == 0) {
    int32_t ent = 0;

    while (cursor.get(&key, &value, MDB_NEXT_NODUP)) {
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
      
      if(storedKey.timeStamp() > _maxTS) break; // break;

      if(storedKey.timeStamp() < prev_TS) {
        std::cout << "Database not plausible: TS not sorted (cur: " << storedKey.timeStamp() << ", prev: " << prev_TS << ")" <<  std::endl;
        break;
      }
      prev_TS = storedKey.timeStamp();

      //std::cout << "in range " << _ts << "; " << _currTS << "; " << storedKey.timeStamp() << std::endl;
      
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

        //if((storedKey.timeStamp() >=  1645101196015660000) && (storedKey.timeStamp() <=  1645101199996113000))
        //  std::cout << storedKey.timeStamp() << "; " << _currAccelLon << "; " << _currAccelTrans << std::endl;

        
        int64_t diffToPrev = storedKey.timeStamp() - lastInFence_TS;

        if((diffToPrev > _maneuver[_dsID]->minDiffTime) && (lastInFence_TS != 0)) { // to make sure, that maneuver is over
          if(flag == true){
              flag = false;
              //if((storedKey.timeStamp() >=  1645101196015660000) && (storedKey.timeStamp() <=  1645101199996113000))
              //  std::cout << "Maneuverdetection end_TS: " << lastInFence_TS << std::endl;
            //if(_maneuver[_dsID]->minDiffTime <= diffToPrev) {
              int64_t duration = lastInFence_TS - start_TS;

              //std::cout << "dur: " << duration << "(" << start_TS << "; " << lastInFence_TS << ")" << std::endl;
              //std::cout << _ts << "; " << _currAccelLon << "; " << _currAccelTrans << std::endl;

              if((duration > _maneuver[_dsID]->minDuration) && (duration < _maneuver[_dsID]->maxDuration)) {
                if (VERBOSE) std::cerr << "Maneuver Stage " << _dsID << " " << start_TS << "  : " << lastInFence_TS << std::endl;
                cursor.close();
                return recursiveManeuverDetector(lastInFence_TS, _dsID+1, _maneuver, env, rotxn, APLX, VERBOSE);
              }
            //}
          } else {
            //flag = false;
          }
        } else { // too many outliers
          //flag = false;
        }
        

        if(in_fence(_maneuver[_dsID]->fenceBL, _maneuver[_dsID]->fenceTR, _currAccelLon, _currAccelTrans) == true) {
          // if(storedKey.timeStamp() == 1645099057959768000) std::cout << "1645099057959768000 in fence; flag = " << flag << " ds: " << _dsID << std::endl;
          // if(storedKey.timeStamp() == 1645099057970052000) std::cout << "1645099057970052000 in fence; flag = " << flag << " ds: " << _dsID << std::endl;

          if(storedKey.timeStamp() < _currTS) { // Maneuver beginnt schon vor Gap min
            //std::cout << "outof range < " << _currTS << std::endl;
            break;
          }

          lastInFence_TS = storedKey.timeStamp();
          lastInFence_accelLon = _currAccelLon;
          lastInFence_accelLat = _currAccelTrans;

          if(flag == false) {
            if(storedKey.timeStamp() > _maxGapTS)
              break;
            else{
              start_TS = storedKey.timeStamp();
              flag = true;
              //if((storedKey.timeStamp() >=  1645101196015660000) && (storedKey.timeStamp() <=  1645101199996113000))
              //  std::cout << "Maneuverdetection start_TS: " << start_TS << std::endl;
            }
          }
        }
        else {
          if((flag == false) && storedKey.timeStamp() > _maxGapTS) {
            break;
          }
        }

          //////////////////////////////////////////

      }
    }
    cursor.close();
    //mdb_cursor_close(cursor);
    //rotxn.abort();

    return 0;
  }


  inline std::vector<std::pair<int64_t, int64_t>> cabinet_queryManeuverBruteForce(const uint64_t &MEM, const std::string &CABINET, const bool &APLX, const bool &VERBOSE, const std::pair<float,float> _fenceBL, const std::pair<float,float> _fenceTR, std::vector<DrivingStatus*> maneuver) {
    bool failed{false};
    std::vector<std::pair<int64_t, int64_t>> maneuverDetectedList;

    try {

      std::pair<float,float> _fenceBL;
      std::pair<float,float> _fenceTR;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////
      

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

      int32_t oldPercentage{-1};
      uint64_t entries{0};
      int loadAnimation = 0;

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
        // if((entries % 20) == 0) {
        //   // loading animation
        //   if (loadAnimation == 0)
        //     std::cout << "\b." << std::flush;
        //   else if(loadAnimation == 1)
        //     std::cout << "\bo" << std::flush;
        //   else if(loadAnimation == 2)
        //     std::cout << "\bO" << std::flush;
        //   else if(loadAnimation == 3)
        //     std::cout << "\bo" << std::flush;

        //   loadAnimation ++;
        //   if(loadAnimation >= 3)
        //     loadAnimation = 0;
        // }


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

          //if((storedKey.timeStamp() >=  1645101196015660000) && (storedKey.timeStamp() <=  1645101199976113000))
          //  std::cout << storedKey.timeStamp() << "; " << _currAccelLon << "; " << _currAccelTrans << std::endl;

          int64_t diffToPrev = storedKey.timeStamp() - lastInFence_TS;

          if((diffToPrev > maneuver[0]->minDiffTime) && (lastInFence_TS != 0)) {
            if(flag == true) {
              flag = false;
              //if(maneuver[0]->minDiffTime <= diffToPrev) {
                int64_t duration = lastInFence_TS - start_TS;
                //std::cout << "dur " << start_TS << "; " << lastInFence_TS << std::endl;

                if((duration > maneuver[0]->minDuration) && (duration < maneuver[0]->maxDuration)) {
                  // hier den nächsten Detektor
                  int64_t end_TS = 0; // detector aufruf
                  
                  //if((storedKey.timeStamp() >=  1645101196015660000) && (storedKey.timeStamp() <=  1645101199976113000))
                  //  std::cout << "call recursive " << lastInFence_TS << std::endl;

                  end_TS = recursiveManeuverDetector(lastInFence_TS, 1, maneuver, env, rotxn, APLX, VERBOSE);

                  //if((storedKey.timeStamp() >=  1645101196015660000) && (storedKey.timeStamp() <=  1645101199976113000))
                  //  std::cout << "end recursive " << lastInFence_TS << std::endl;

                  if(end_TS != 0){
                    //std::cout << "Maneuver " << start_TS << ", " << lastInFence_TS << std::endl;
                    // maneuver speichern (start, end)
                    maneuverDetectedList.push_back(std::make_pair(start_TS, end_TS));
                    if (VERBOSE) std::cerr << "Maneuver at " << start_TS << "  : " << lastInFence_TS << std::endl;
                  }
                }
              //}
            } else {
              // flag = false;
            }
          } else {
            //flag = false;
          }
          
          if(in_fence(maneuver[0]->fenceBL, maneuver[0]->fenceTR, _currAccelLon, _currAccelTrans) == true) {

            
            lastInFence_TS = storedKey.timeStamp();
            lastInFence_accelLon = _currAccelLon;
            lastInFence_accelLat = _currAccelTrans;

            if(flag == false) {
              start_TS = storedKey.timeStamp();
              flag = true;

              //if((storedKey.timeStamp() >=  1645101196015660000) && (storedKey.timeStamp() <=  1645101199976113000))
              //  std::cout << "start man " << start_TS << std::endl;
            }
          }

        }
      }

      cursor.close();
      //mdb_cursor_close(cursor);
      rotxn.abort();
      return maneuverDetectedList;
    }

    catch (...) {
      failed = true;
    }
  }

//} // namespace brute_force

#endif
