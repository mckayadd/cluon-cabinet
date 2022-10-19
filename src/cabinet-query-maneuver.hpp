/*
 * Copyright (C) 2022  Lukas Birkemeyer
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef CABINETQUERYACCELFROMMORTON_HPP
#define CABINETQUERYACCELFROMMORTON_HPP

#include "DrivingStatus.hpp"
#include "cluon-complete.hpp"
#include "key.hpp"
#include "db.hpp"
#include "lmdb.h"
#include "morton.hpp"
#include "opendlv-standard-message-set.hpp"

#include <cstdio>
#include <cstring>
#include <cstdint>

#include <iostream>
#include <iomanip>
#include <string>

inline int32_t singleNonRelevantMortonBin(const std::pair<float,float> &BoxBL, const std::pair<float,float> &BoxTR, std::vector<uint64_t> * _nonRelevantMorton) {
  int32_t retCode{0};

  _nonRelevantMorton->clear();

  uint64_t bl_morton = 0;
  uint64_t tr_morton = 0;

  bl_morton = convertAccelLonTransToMorton(BoxBL);
  tr_morton = convertAccelLonTransToMorton(BoxTR);

  const uint32_t _xBL = std::lroundf((BoxBL.first + 10.0f) * 100.0f);
  const uint32_t _yBL = std::lround((BoxBL.second + 10.0f) * 100.0f);
  const uint32_t _xTR = std::lroundf((BoxTR.first + 10.0f) * 100.0f);
  const uint32_t _yTR = std::lround((BoxTR.second + 10.0f) * 100.0f);

  // add all values within morton ares to vector
  for (int i=bl_morton; i<= tr_morton; i++) {
    _nonRelevantMorton->push_back(i);
  }

  // remove morton values that are not covered by the fence -> only values that are not relevant remain
  int i, j;
  std::pair<float,float> _accelbox;
  uint64_t _morton;
  for (i=_yBL; i<_yTR; i++) {
    for (j=_xBL; j<_xTR; j++) {
      _accelbox.first = j/100.0f - 10.0f;
      _accelbox.second = i/100.0f - 10.0f;
      _morton = convertAccelLonTransToMorton(_accelbox);
      _nonRelevantMorton->erase(std::remove(_nonRelevantMorton->begin(), _nonRelevantMorton->end(), _morton), _nonRelevantMorton->end());
      //std::cout << _morton << std::endl;
    }
  }

  return retCode;
}


inline int32_t identifyRelevantMortonBins(const std::pair<float,float> &BoxBL, const std::pair<float,float> &BoxTR, std::vector<uint64_t> * _relevantMorton) {
  int32_t retCode{0};

  uint64_t bl_morton = 0;
  uint64_t tr_morton = 0;

  bl_morton = convertAccelLonTransToMorton(BoxBL);
  tr_morton = convertAccelLonTransToMorton(BoxTR);

  const uint32_t _xBL = std::lroundf((BoxBL.first + 10.0f) * 100.0f);
  const uint32_t _yBL = std::lround((BoxBL.second + 10.0f) * 100.0f);
  const uint32_t _xTR = std::lroundf((BoxTR.first + 10.0f) * 100.0f);
  const uint32_t _yTR = std::lround((BoxTR.second + 10.0f) * 100.0f);

  //std::cout << BoxBL.first << ", " << _xBL << "; " << BoxBL.second << ", " << _yBL << std::endl;
  //std::cout << BoxTR.first << ", " << _xTR << "; " << BoxTR.second << ", " << _yTR << std::endl;

  int i, j;
  std::pair<float,float> _accelbox;
  uint64_t _morton;
  for (i=_yBL; i <=_yTR; i++) {
    for (j=_xBL; j <=_xTR; j++) {
      _accelbox.first = j/100.0f - 10.0f;
      _accelbox.second = i/100.0f - 10.0f;
      _morton = convertAccelLonTransToMorton(_accelbox);
      _relevantMorton->push_back(_morton);
      //if(_morton == 2485778)
      //  std::cout << _morton << "; (" << j << ", " << i << ") ; " << _accelbox.first << "," << _accelbox.second  << std::endl;
    }
  }

  return retCode;
}
 
inline bool cmp_sort_first(const std::pair<int64_t,int64_t> & lhs, 
         const std::pair<int64_t,int64_t> & rhs)
{
  return lhs.second < rhs.second;
}

inline bool cmp_sort(const int64_t& lhs, 
         const int64_t & rhs)
{
  return lhs < rhs;
}

inline std::vector<std::pair<int64_t,int64_t>> detectSingleManeuver(std::vector<int64_t> * _tempDrivingStatusList, int64_t minDiffTime, int64_t minDuration, int64_t maxduration) {
  int32_t retCode{0};
  
  sort(_tempDrivingStatusList->begin(), _tempDrivingStatusList->end(), cmp_sort);

  int64_t _tsStart = 0;
  int64_t _tsEnd = 0;

  std::vector<std::pair<int64_t,int64_t>> _singleManeuverList;

  for(int i=0; i < _tempDrivingStatusList->size(); i++) {
    if(0==i){
      _tsStart = (*_tempDrivingStatusList)[i];
      continue;
    }

    //if(((*_tempDrivingStatusList)[i] >= 1590992666780594000) && ((*_tempDrivingStatusList)[i] <= 1590992667650156000))
    //std::cout << (*_tempDrivingStatusList)[i] << "; " << (*_tempDrivingStatusList)[i-1] << "; " << (*_tempDrivingStatusList)[i] - (*_tempDrivingStatusList)[i-1] << std::endl;

    if((minDiffTime < (*_tempDrivingStatusList)[i] - (*_tempDrivingStatusList)[i-1]) || (i == (_tempDrivingStatusList->size()-1))) {
      
      if(i == (_tempDrivingStatusList->size()-1)) _tsEnd = (*_tempDrivingStatusList)[i];
      else _tsEnd = (*_tempDrivingStatusList)[i-1];

      
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


inline int64_t maneuverDetectorRecursiv(std::vector<DrivingStatus*> maneuver, int maneuver_idx, int status_idx) {
  
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
        return maneuverDetectorRecursiv(maneuver, maneuver_idx + 1, i);
      }
    }
  }

  return -1;
}

inline std::vector<std::pair<int64_t, int64_t>> identifyManeuversSFC(char **argv, const std::string CABINET, const uint64_t MEM, const bool VERBOSE, const uint64_t THR, const bool APLX, std::vector<std::string> geoboxStrings, std::pair<float,float> geoboxBL, std::pair<float,float> geoboxTR, std::vector<DrivingStatus*> maneuver) {
  int32_t retCode{0};

  // Maneuver Detection
  std::pair<int64_t, int64_t> fullManeuver;
  std::vector<std::pair<int64_t, int64_t>> fullManeuverList;

  MDB_env *env{nullptr};
  const int numberOfDatabases{100};
  const int64_t SIZE_DB = MEM * 1024UL * 1024UL * 1024UL;

  // lambda to check the interaction with the database.
  auto checkErrorCode = [_argv=argv](int32_t rc, int32_t line, std::string caller) {
    if (0 != rc) {
      std::cerr << "[" << _argv[0] << "]: " << caller << ", line " << line << ": (" << rc << ") " << mdb_strerror(rc) << std::endl; 
    }
    return (0 == rc);
  };

  if (!checkErrorCode(mdb_env_create(&env), __LINE__, "mdb_env_create")) {
    return fullManeuverList;
  }
  if (!checkErrorCode(mdb_env_set_maxdbs(env, numberOfDatabases), __LINE__, "mdb_env_set_maxdbs")) {
    mdb_env_close(env);
    return fullManeuverList;
  }
  if (!checkErrorCode(mdb_env_set_mapsize(env, SIZE_DB), __LINE__, "mdb_env_set_mapsize")) {
    mdb_env_close(env);
    return fullManeuverList;
  }
  if (!checkErrorCode(mdb_env_open(env, CABINET.c_str(), MDB_NOSUBDIR|MDB_RDONLY, 0600), __LINE__, "mdb_env_open")) {
    mdb_env_close(env);
    return fullManeuverList;
  }
  
  {
    MDB_txn *txn{nullptr};
    MDB_dbi dbi{0};
    if (!checkErrorCode(mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn), __LINE__, "mdb_txn_begin")) {
      mdb_env_close(env);
      return fullManeuverList;
    }
    retCode = APLX ? mdb_dbi_open(txn, "533/0-morton", 0 , &dbi) : mdb_dbi_open(txn, "1030/2-morton", 0 , &dbi);
    //retCode = mdb_dbi_open(txn, "533/0-morton", 0 , &dbi);
    //retCode = mdb_dbi_open(txn, "1030/2-morton", 0 , &dbi);
    if (MDB_NOTFOUND  == retCode) {
      if(APLX){std::clog << "[" << argv[0] << "]: No database '533/0-morton' found in " << CABINET << "." << std::endl;}
      else{std::clog << "[" << argv[0] << "]: No database '1030/2-morton' found in " << CABINET << "." << std::endl;}
    }
    else {
      mdb_set_compare(txn, dbi, &compareMortonKeys);
      // Multiple values are stored by existing timeStamp in nanoseconds.
      mdb_set_dupsort(txn, dbi, &compareKeys);

      uint64_t numberOfEntries{0};
      MDB_stat stat;
      if (!mdb_stat(txn, dbi, &stat)) {
        numberOfEntries = stat.ms_entries;
      }
      if(APLX){std::clog << "[" << argv[0] << "]: Found " << numberOfEntries << " entries in database '533/0-morton' in " << CABINET << std::endl;}
      else{std::clog << "[" << argv[0] << "]: Found " << numberOfEntries << " entries in database '1030/2-morton' in " << CABINET << std::endl;}

      uint64_t bl_morton = 0;
      uint64_t tr_morton = 0;
      std::vector<uint64_t> relevantMorton;
      

      // Query with Threshold
      if (0 != THR) {
        bl_morton = THR;
        tr_morton = std::numeric_limits<uint64_t>::max()-1;
        std::clog << "[" << argv[0] << "]: Morton code threshold: " <<  bl_morton << std::endl;
      }
      else {
        if (4 == geoboxStrings.size()) { // no geobox argument

          DrivingStatus *_tempDS = new DrivingStatus( "geofence",
              geoboxBL,
              geoboxTR,
              100000000,
              3000000000,
              0,
              0,
              160000000);

          maneuver.push_back(_tempDS);
        }

        for(DrivingStatus* _DrivingStatus : maneuver) {
          identifyRelevantMortonBins(_DrivingStatus->fenceBL, _DrivingStatus->fenceTR, &(_DrivingStatus->relevantMorton));
          if(VERBOSE){
            std::clog << "Identification of relevant Areas for " << _DrivingStatus->name << "." << std::endl;
            std::clog << "Searchmask contains " << _DrivingStatus->relevantMorton.size() << " values." << std::endl;
          }
        }

      }

      
      MDB_cursor *cursor;
      if (!(retCode = mdb_cursor_open(txn, dbi, &cursor))) {
        MDB_val key;
        MDB_val value;

        for(DrivingStatus* _tempDS : maneuver) {
          std::vector<int64_t> _tempDrivingStatusList;

          if(VERBOSE)
            std::cout << "Work on: " << _tempDS->name << std::endl;

          for (uint64_t _relevantMorton : _tempDS->relevantMorton) {

            //auto decodedAccel = convertMortonToAccelLonTrans(_relevantMorton);
            //std::cout << std::setprecision(10) << decodedAccel.first << ";" << decodedAccel.second << std::endl;
            

            key.mv_size = sizeof(_relevantMorton);
            auto _temp_relevantMorton = htobe64(_relevantMorton);
            key.mv_data = &_temp_relevantMorton;

            //uint64_t temp = *reinterpret_cast<uint64_t*>(key.mv_data);
            //temp = be64toh(temp);
            //std::clog << "Key" << key.mv_data << "; " << &_temp_relevantMorton << "; changed " <<  temp << "; " << _relevantMorton << std::endl;


            //for (auto _tempPair : nonRelevantMortonBins) {
            if (MDB_NOTFOUND != mdb_cursor_get(cursor, &key, &value, MDB_SET_RANGE)) {
            // for (size_t _relevantMortonIter = 0; _relevantMortonIter < relevantMorton.size(); ++_relevantMortonIter) {
            //   uint64_t _relevantMorton = relevantMorton[_relevantMortonIter];
              // if (_relevantMorton == 16119181463)

              //auto _tempRelevantMorton = htobe64(_relevantMorton);
              //key.mv_data = &_tempRelevantMorton;

              int _rc = 0;

              while (_rc == 0) {

                uint64_t morton = *reinterpret_cast<uint64_t*>(key.mv_data);
                morton = be64toh(morton);
                //uint64_t _tempRelMort = be64toh(_relevantMorton);

                if(morton > _relevantMorton) break; // break;
                
                //std::clog << "Morton: " << morton << "; relevant_value: " << _relevantMorton << "; TS: " << *reinterpret_cast<uint64_t*>(key.mv_data) << std::endl;

                auto decodedAccel = convertMortonToAccelLonTrans(morton);
                int64_t timeStamp{0};
                if (value.mv_size == sizeof(int64_t)) {
                  const char *ptr = static_cast<char*>(value.mv_data);
                  std::memcpy(&timeStamp, ptr, value.mv_size);
                  timeStamp = be64toh(timeStamp);
                  //if (VERBOSE) {
                  //  std::cout << bl_morton << ";" << morton << ";" << tr_morton << ";";
                  //if((timeStamp >= 1590992666780594000) && (timeStamp <= 1590992667650156000))
                  //  std::cout << timeStamp << "; " << morton << "; " << decodedAccel.first << "; " << decodedAccel.second << std::endl;
                  //}
                  
                  //store morton timeStamp combo
                  // std::pair<uint64_t,int64_t> _mortonTS;
                  // _mortonTS.first = morton;
                  // _mortonTS.second = timeStamp;
                  // DrivingStatusList.push_back(_mortonTS);
                  _tempDrivingStatusList.push_back(timeStamp);
                }
                // cursor weitersetzen
                _rc = mdb_cursor_get(cursor, &key, &value, MDB_NEXT);
              }
            }
          }

        _tempDS->singleManeuverList = detectSingleManeuver(&_tempDrivingStatusList, _tempDS->minDiffTime, _tempDS->minDuration, _tempDS->maxDuration);
        sort(_tempDS->singleManeuverList.begin(), _tempDS->singleManeuverList.end(), cmp_sort_first);
        if(VERBOSE) {
          std::cout << "Found " << _tempDS->singleManeuverList.size() << " " << _tempDS->name << std::endl;
          for(auto temp : _tempDS->singleManeuverList)
            std::cout << "Start " << temp.first << "; End " << temp.second << std::endl;
        }
        }
      }
    }
    mdb_txn_abort(txn);
    if (dbi) {
      mdb_dbi_close(env, dbi);
    }
  }
  
  for(int status_idx = 0; status_idx < maneuver[0]->singleManeuverList.size(); status_idx++) {
    fullManeuver.second = maneuverDetectorRecursiv(maneuver, 0, status_idx);

    if(fullManeuver.second != -1){
      fullManeuver.first = maneuver[0]->singleManeuverList[status_idx].first;
      //if(VERBOSE)
      //  std::cout << "Maneuver detected! Start: " << fullManeuver.first << ", End: " << fullManeuver.second << std::endl;

      fullManeuverList.push_back(fullManeuver);
    }
  }
  //std::cout << "We detected " << fullManeuverList.size() << " Maneuvers." << std::endl;

  if (env) {
    mdb_env_close(env);
  }

  return fullManeuverList;
}


#endif