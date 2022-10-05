/*
 * Copyright (C) 2022  Lukas Birkemeyer
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"
#include "cabinet-query-maneuver.hpp"
#include "DrivingStatus.hpp"

#include <cstdio>
#include <cstring>
#include <cstdint>

#include <iostream>
#include <iomanip>
#include <string>


int64_t maneuverDetectorRecursiv(std::vector<DrivingStatus*> maneuver, int maneuver_idx, int status_idx) {
  
  if(maneuver.size() == 1) {
    return maneuver[maneuver_idx]->singleManeuverList[status_idx].second;
  }

  if( maneuver_idx >= maneuver.size())
    return -1;

  if(maneuver.size() == 1)
    return -1;

  // std::cout << "start maneuver Detection: maneuver idx " << maneuver_idx << " status " << status_idx << std::endl;
  
  DrivingStatus* currentManeuver = maneuver[maneuver_idx];
  DrivingStatus* nextManeuver = maneuver[maneuver_idx + 1];

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
        maneuverDetectorRecursiv(maneuver, maneuver_idx + 1, i);
      }
    }
  }

  return -1;
}


int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if ( (0 == commandlineArguments.count("cab"))) {
    std::cerr << argv[0] << " query a cabinet (an lmdb-based key/value-database)." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --cab=myStore.cab [--mem=32024] --geobox=bottom-left-latitude,bottom-left-longitude,top-right-latitude,top-right-longitude" << std::endl;
    std::cerr << "         --cab:    name of the database file" << std::endl;
    std::cerr << "         --mem:    upper memory size for database in memory in GB, default: 64,000 (representing 64TB)" << std::endl;
    std::cerr << "         --thr:    lower threshold in morton space e.g. 31634000000 or 33400000000 (emergency braking); alternative to --geobox (thr is prioritized)" << std::endl;
    std::cerr << "         --geobox: return all timeStamps for GPS locations within this rectangle specified by bottom-left and top-right lat/longs" << std::endl;
    std::cerr << "         --aplx:   Applenix data required (e.g. Snowfox)? default: no (e.g. Voyager)" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cab=myStore.cab --geobox=57.679000,12.309931,57.679690,12.312700" << std::endl;
    retCode = 1;
  } else {    
    const std::string CABINET{commandlineArguments["cab"]};
    const uint64_t MEM{(commandlineArguments["mem"].size() != 0) ? static_cast<uint64_t>(std::stoi(commandlineArguments["mem"])) : 64UL*1024UL};
    const bool VERBOSE{commandlineArguments["verbose"].size() != 0};
    const uint64_t THR{(commandlineArguments.count("thr") != 0) ? static_cast<uint64_t>(std::stol(commandlineArguments["thr"])) : 0};
    const std::string GEOBOX{commandlineArguments["geobox"]};
    const bool APLX{commandlineArguments["aplx"].size() != 0};
    std::vector<std::string> geoboxStrings = stringtoolbox::split(GEOBOX, ',');
    std::pair<float,float> geoboxBL;
    std::pair<float,float> geoboxTR;
    if (4 == geoboxStrings.size()) {
      geoboxBL.first = std::stof(geoboxStrings.at(0));
      geoboxBL.second = std::stof(geoboxStrings.at(1));
      geoboxTR.first = std::stof(geoboxStrings.at(2));
      geoboxTR.second = std::stof(geoboxStrings.at(3));
    }

    std::pair<float,float> _fenceBL;
    std::pair<float,float> _fenceTR;

////////////////////////////////////////////////////////////////////////////////

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
    
    //maneuver.push_back(leftCurve);
    maneuver.push_back(rightCurve);
    //maneuver.push_back(harsh_braking);

////////////////////////////////////////////////////////////////////////////////

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
      return 1;
    }
    if (!checkErrorCode(mdb_env_set_maxdbs(env, numberOfDatabases), __LINE__, "mdb_env_set_maxdbs")) {
      mdb_env_close(env);
      return 1;
    }
    if (!checkErrorCode(mdb_env_set_mapsize(env, SIZE_DB), __LINE__, "mdb_env_set_mapsize")) {
      mdb_env_close(env);
      return 1;
    }
    if (!checkErrorCode(mdb_env_open(env, CABINET.c_str(), MDB_NOSUBDIR|MDB_RDONLY, 0600), __LINE__, "mdb_env_open")) {
      mdb_env_close(env);
      return 1;
    }
    
    {
      MDB_txn *txn{nullptr};
      MDB_dbi dbi{0};
      if (!checkErrorCode(mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn), __LINE__, "mdb_txn_begin")) {
        mdb_env_close(env);
        return (retCode = 1);
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
            std::clog << "Identification of relevant Areas for " << _DrivingStatus->name << "." << std::endl;
            identifyRelevantMortonBins(_DrivingStatus->fenceBL, _DrivingStatus->fenceTR, &(_DrivingStatus->relevantMorton));
            std::clog << "Searchmask contains " << _DrivingStatus->relevantMorton.size() << " values." << std::endl;
          }

        }

        
        MDB_cursor *cursor;
        if (!(retCode = mdb_cursor_open(txn, dbi, &cursor))) {
          MDB_val key;
          MDB_val value;

          for(DrivingStatus* _tempDS : maneuver) {
            std::vector<int64_t> _tempDrivingStatusList;

            std::cout << "Work on: " << _tempDS->name << std::endl;

            for (uint64_t _relevantMorton : _tempDS->relevantMorton) {

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
                    if (VERBOSE) {
                      std::cout << bl_morton << ";" << morton << ";" << tr_morton << ";";
                      std::cout << std::setprecision(10) << decodedAccel.first << ";" << decodedAccel.second << ";" << timeStamp << std::endl;
                    }
                    
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
          std::cout << "Found " << _tempDS->singleManeuverList.size() << " " << _tempDS->name << std::endl;
          //for(auto temp : _tempDS->singleManeuverList)
          //  std::cout << "Start " << temp.first << "; End " << temp.second << std::endl;
          }
        }
      }
      mdb_txn_abort(txn);
      if (dbi) {
        mdb_dbi_close(env, dbi);
      }
    }

    // Maneuver Detection
    std::pair<int64_t, int64_t> fullManeuver;
    std::vector<std::pair<int64_t, int64_t>> fullManeuverList;
    
    for(int status_idx = 0; status_idx < maneuver[0]->singleManeuverList.size(); status_idx++) {
      fullManeuver.second = maneuverDetectorRecursiv(maneuver, 0, status_idx);

      if(fullManeuver.second != -1){
        fullManeuver.first = maneuver[0]->singleManeuverList[status_idx].first;
        std::cout << "Maneuver detected! Start: " << fullManeuver.first << ", End: " << fullManeuver.second << std::endl;

        fullManeuverList.push_back(fullManeuver);
      }
    }
    std::cout << "We detected " << fullManeuverList.size() << " Maneuvers." << std::endl;

    if (env) {
      mdb_env_close(env);
    }
  }
  return retCode;
}