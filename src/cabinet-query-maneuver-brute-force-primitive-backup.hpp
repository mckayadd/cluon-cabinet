/*
 * Copyright (C) 2022  Lukas Birkemeyer
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef CABINETQUERYMANEUVERBRUTEFORCEPRIMITIVE_HPP
#define CABINETQUERYMANEUVERBRUTEFORCEPRIMITIVE_HPP

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


inline std::vector<std::pair<int64_t,int64_t>> detectSingleManeuver_primitive(std::vector<int64_t> * _tempDrivingStatusList, int64_t minDiffTime, int64_t minDuration, int64_t maxduration) {
  int32_t retCode{0};
  
  //not required, since original database is sorted
  //sort(_tempDrivingStatusList->begin(), _tempDrivingStatusList->end(), cmp_sort);

  int64_t _tsStart = 0;
  int64_t _tsEnd = 0;

  std::vector<std::pair<int64_t,int64_t>> _singleManeuverList;

  for(int i=0; i < _tempDrivingStatusList->size(); i++) {
    if(0==i){
      _tsStart = (*_tempDrivingStatusList)[i];
      continue;
    }

    //if(((*_tempDrivingStatusList)[i] >= 1645098108930034000) && ((*_tempDrivingStatusList)[i] <= 1645098112159906000))
    //  std::cout << (*_tempDrivingStatusList)[i] << "; " << (*_tempDrivingStatusList)[i-1] << "; " << (*_tempDrivingStatusList)[i] - (*_tempDrivingStatusList)[i-1] << std::endl;

    if((minDiffTime < (*_tempDrivingStatusList)[i] - (*_tempDrivingStatusList)[i-1]) || (i == (_tempDrivingStatusList->size()-1))) {
      
      _tsEnd = (*_tempDrivingStatusList)[i-1];

      if(i == (_tempDrivingStatusList->size()-1) && (minDiffTime > (*_tempDrivingStatusList)[i] - (*_tempDrivingStatusList)[i-1]))
      {
        _tsEnd = (*_tempDrivingStatusList)[i];
      }

      
      int64_t duration = _tsEnd - _tsStart;
      //std::cout << duration << "; " << (*_tempDrivingStatusList)[i] << "; " << abs((*_tempDrivingStatusList)[i] - (*_tempDrivingStatusList)[i-1]) << std::endl;
      
      if((duration > minDuration) && (duration < maxduration)) {
        std::pair<int64_t,int64_t> _tempMan;
        _tempMan.first = _tsStart;
        _tempMan.second = _tsEnd;

        _singleManeuverList.push_back(_tempMan);
      }

      _tsStart = (*_tempDrivingStatusList)[i];
      // TODO: irgendwas mit Start ist kaputt
    }
  }

  return _singleManeuverList;
}

// namespace brute_force {

  inline bool in_fence_primitive(const std::pair<float,float> _fenceBL, const std::pair<float,float> _fenceTR, const float _x, const float _y) {
    if((_x >= _fenceBL.first) && (_x <= _fenceTR.first)){
      if((_y >= _fenceBL.second) && (_y <= _fenceTR.second)){
        return true;
      }
    }
    return false;
  }

  inline int64_t maneuverDetectorRecursiv_primtive(std::vector<DrivingStatus*> maneuver, int maneuver_idx, int status_idx) {
  
    if(maneuver.size() == 1) {
      return maneuver[maneuver_idx]->singleManeuverList[status_idx].second;
    }

    if( maneuver_idx >= maneuver.size())
      return -1;

    //if(maneuver.size() == 1)
    //  return 0;

    // std::cout << "start maneuver Detection: maneuver idx " << maneuver_idx << " status " << status_idx << std::endl;
    
    DrivingStatus* currentManeuver = maneuver[maneuver_idx];
    DrivingStatus* nextManeuver = maneuver[maneuver_idx + 1];

    if(nextManeuver->singleManeuverList.size() == 0)
      return -1;

    std::pair<int64_t,int64_t> currentStatus = currentManeuver->singleManeuverList[status_idx];

    //for(std::pair<int64_t,int64_t> nextStatus : nextManeuver->singleManeuverList) 
    
    for(int i = 0; i <= nextManeuver->singleManeuverList.size(); i++) {

      std::pair<int64_t,int64_t> nextStatus = nextManeuver->singleManeuverList[i];

      int64_t gap = nextStatus.first - currentStatus.second;

      //std::cout << "Gap: " << gap << " first " << nextStatus.first << " end " << currentStatus.second << std::endl;

      if(gap > currentManeuver->maxGap){
        // std::cout << "gap " << gap << " groesser als " <<  currentManeuver->maxGap << std::endl;
        break;
      }

      if (gap < currentManeuver->minGap) continue;

      if((gap > currentManeuver->minGap) && (gap < currentManeuver->maxGap)) {
        
        if(maneuver_idx == (maneuver.size() - 2)) {
          return nextStatus.second;
        } else {
          return maneuverDetectorRecursiv_primtive(maneuver, maneuver_idx + 1, i);
        }
      }
    }

    return -1;
  }

  

  inline std::vector<std::pair<int64_t, int64_t>> cabinet_queryManeuverBruteForcePrimitive(const uint64_t &MEM, const std::string &CABINET, const bool &APLX, 
                                                                                          const bool &VERBOSE, const std::pair<float,float> _fenceBL, 
                                                                                          const std::pair<float,float> _fenceTR, 
                                                                                          std::vector<DrivingStatus*> _maneuver, 
                                                                                          uint64_t db_start, uint64_t db_end, 
                                                                                          uint64_t &cntEntries) 
  
  {
    bool failed{false};

    // Maneuver Detection
    std::pair<int64_t, int64_t> fullManeuver;
    std::vector<std::pair<int64_t, int64_t>> fullManeuverList;

    try {

      // std::cout<< "####### " << _fenceBL.first << " " << _fenceBL.second << "  ---  " << _fenceTR.first << " " << _fenceTR.second << std::endl;
      // std::pair<float,float> _fenceBL;
      // std::pair<float,float> _fenceTR;
      // std::cout<< "####### Before function call: " << _fenceBL.first << " " << _fenceBL.second << "  ---  " << _fenceTR.first << " " << _fenceTR.second << std::endl;
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////
      

          ///////////////////////////////////////////////////////////////////////////////////////////////////////////

      uint16_t _ID = APLX ? opendlv::device::gps::pos::Grp1Data::ID() : opendlv::proxy::AccelerationReading::ID();
      // std::string DB = APLX ? "533/0" : "1030/2";
      std::string DB = "1030/2";
      std::cout << "DB: " << DB << std::endl;
      auto env = lmdb::env::create();
      env.set_mapsize(MEM * 1024UL * 1024UL * 1024UL);
      env.set_max_dbs(100);
      env.open(CABINET.c_str(), MDB_NOSUBDIR, 0600);
      // Fetch key/value pairs in a read-only transaction.
      auto rotxn = lmdb::txn::begin(env, nullptr, MDB_RDONLY);
      auto dbiAll = lmdb::dbi::open(rotxn, "all");
     
      dbiAll.set_compare(rotxn, &compareKeys);
      if(VERBOSE) std::clog << "Found " << dbiAll.size(rotxn) << " entries in database 'all'." << std::endl;
      auto dbi = lmdb::dbi::open(rotxn, DB.c_str());
      dbi.set_compare(rotxn, &compareKeys);
      const uint64_t totalEntries = dbi.size(rotxn) * _maneuver.size();
      if(VERBOSE) std::clog << "Found " << totalEntries << " entries in database '" << DB << "'." << std::endl;

      //auto cursor = lmdb::cursor::open(rotxn, dbi);

      MDB_val key;
      MDB_val value;

      int32_t oldPercentage{-1};
      uint64_t entries{0};
      db_start = db_start == 0 ? 0 : db_start;
      db_end = db_end == 0 ? 1646827840115619000 : db_end;

      //while (mdb_cursor_get(cursor, &key, &value, MDB_NEXT_NODUP) == 0) {
      bool firstFlag = true;

      for(DrivingStatus* _tempDS : _maneuver) {

        std::vector<int64_t> _tempDrivingStatusList;

        if(VERBOSE)
          std::cout << "Work on: " << _tempDS->name << std::endl;
        
        auto cursor = lmdb::cursor::open(rotxn, dbi);
        while (cursor.get(&key, &value, MDB_NEXT)) {

          const int32_t percentage = static_cast<int32_t>((static_cast<float>(entries) * 100.0f) / static_cast<float>(totalEntries));
          if ((percentage % 5 == 0) && (percentage != oldPercentage)) {
            std::clog <<"Processed " << percentage << "% (" << entries/_maneuver.size() << " entries) from " << CABINET << std::endl;
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
            // const auto tmp = cluon::extractMessage<opendlv::device::gps::pos::Grp1Data>(std::move(e.second));

            if(storedKey.timeStamp() < db_start)
              continue;
            if(storedKey.timeStamp() > db_end){
              break;
            }

            if(firstFlag) cntEntries++;

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

            if(in_fence_primitive(_tempDS->fenceBL, _tempDS->fenceTR, _currAccelLon, _currAccelTrans) == true) {
              // todo
              _tempDrivingStatusList.push_back(storedKey.timeStamp());
            }

            //not required since original database is sorted
            //sort(_tempDS->singleManeuverList.begin(), _tempDS->singleManeuverList.end(), cmp_sort_first);
            //if(VERBOSE) {
            //  std::cout << "Found " << _tempDS->singleManeuverList.size() << " " << _tempDS->name << std::endl;
              //for(auto temp : _tempDS->singleManeuverList)
              //  std::cout << "Start " << temp.first << "; End " << temp.second << std::endl;
            //}

          }
        }
        _tempDS->singleManeuverList = detectSingleManeuver_primitive(&_tempDrivingStatusList, _tempDS->minDiffTime, _tempDS->minDuration, _tempDS->maxDuration);

        cursor.close();
        firstFlag = false;
      }

      for(int status_idx = 0; status_idx < _maneuver[0]->singleManeuverList.size(); status_idx++) {
        fullManeuver.second = maneuverDetectorRecursiv_primtive(_maneuver, 0, status_idx);
        // std::cout<<"x35"<<std::endl;

        if(fullManeuver.second != -1){
          fullManeuver.first = _maneuver[0]->singleManeuverList[status_idx].first;
          
          fullManeuverList.push_back(fullManeuver);
        }
        // std::cout<<"size: " << fullManeuverList.size() <<std::endl;
      }

      //cursor.close();
      //mdb_cursor_close(cursor);
      rotxn.abort();
      std::cout<<"Primitive!!!!!!!!!!!!!!!"<<std::endl;
      return fullManeuverList;
    }

    catch (...) {
      failed = true;
    }
  }

//} // namespace brute_force

#endif
