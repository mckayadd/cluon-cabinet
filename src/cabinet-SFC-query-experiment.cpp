/*
 * Copyright (C) 2022  Lukas Birkemeyer
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"
#include "cabinet-SFC-query-experiment.hpp"
#include "DrivingStatus.hpp"

#include <cstdio>
#include <cstring>
#include <cstdint>

#include <chrono>

#include <iostream>
#include <iomanip>
#include <string>


int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if ( (0 == commandlineArguments.count("cab_bf")) || (0 == commandlineArguments.count("cab_sfc"))) {
    std::cerr << argv[0] << " query a cabinet (an lmdb-based key/value-database)." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --cab=myStore.cab [--mem=32024] --geobox=bottom-left-latitude,bottom-left-longitude,top-right-latitude,top-right-longitude" << std::endl;
    std::cerr << "         --cab_bf:    name of the database file for BruteForce query" << std::endl;
    std::cerr << "         --cab_sfc:    name of the database file for SFC query" << std::endl;
    std::cerr << "         --mem:    upper memory size for database in memory in GB, default: 64,000 (representing 64TB)" << std::endl;
    std::cerr << "         --thr:    lower threshold in morton space e.g. 31634000000 or 33400000000 (emergency braking); alternative to --geobox (thr is prioritized)" << std::endl;
    std::cerr << "         --geobox: return all timeStamps for GPS locations within this rectangle specified by bottom-left and top-right lat/longs" << std::endl;
    std::cerr << "         --aplx:   Applenix data required (e.g. Snowfox)? default: no (e.g. Voyager)" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cab=myStore.cab --geobox=57.679000,12.309931,57.679690,12.312700" << std::endl;
    retCode = 1;
  } else {    
    const std::string CABINET{commandlineArguments["cab_bf"]};
    const std::string CABINET_SFC{commandlineArguments["cab_sfc"]};
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
// Check Database consitencies
/*
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
    return retCode;
  }
  if (!checkErrorCode(mdb_env_set_maxdbs(env, numberOfDatabases), __LINE__, "mdb_env_set_maxdbs")) {
    mdb_env_close(env);
    return retCode;
  }
  if (!checkErrorCode(mdb_env_set_mapsize(env, SIZE_DB), __LINE__, "mdb_env_set_mapsize")) {
    mdb_env_close(env);
    return retCode;
  }
  // Database_BF
  if (!checkErrorCode(mdb_env_open(env, CABINET.c_str(), MDB_NOSUBDIR|MDB_RDONLY, 0600), __LINE__, "mdb_env_open")) {
    mdb_env_close(env);
    return retCode;
  }
  
  MDB_txn *txn{nullptr};
  MDB_dbi dbi{0};
  if (!checkErrorCode(mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn), __LINE__, "mdb_txn_begin")) {
    mdb_env_close(env);
    return retCode;
  }
  retCode = APLX ? mdb_dbi_open(txn, "533/0", 0 , &dbi) : mdb_dbi_open(txn, "1030/2", 0 , &dbi);
  if (MDB_NOTFOUND  == retCode) {
    if(APLX){std::clog << "[" << argv[0] << "]: No database '533/0' found in " << CABINET << "." << std::endl;}
    else{std::clog << "[" << argv[0] << "]: No database '1030/2' found in " << CABINET << "." << std::endl;}

    return retCode;
  }

  mdb_set_compare(txn, dbi, &compareMortonKeys);
  // Multiple values are stored by existing timeStamp in nanoseconds.
  mdb_set_dupsort(txn, dbi, &compareKeys);

  uint64_t numberOfEntries_BF{0};
  MDB_stat stat;
  if (!mdb_stat(txn, dbi, &stat)) {
    numberOfEntries_BF = stat.ms_entries;
  }
  if(APLX){std::clog << "[" << argv[0] << "]: Found " << numberOfEntries_BF << " entries in database '533/0' in " << CABINET << std::endl;}
  else{std::clog << "[" << argv[0] << "]: Found " << numberOfEntries_BF << " entries in database '1030/2' in " << CABINET << std::endl;}


 // Database_SFC
  if (!checkErrorCode(mdb_env_open(env, CABINET_SFC.c_str(), MDB_NOSUBDIR|MDB_RDONLY, 0600), __LINE__, "mdb_env_open")) {
    mdb_env_close(env);
    return retCode;
  }
  
  if (!checkErrorCode(mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn), __LINE__, "mdb_txn_begin")) {
    mdb_env_close(env);
    return retCode;
  }
  retCode = APLX ? mdb_dbi_open(txn, "533/0-morton", 0 , &dbi) : mdb_dbi_open(txn, "1030/2-morton", 0 , &dbi);
  if (MDB_NOTFOUND  == retCode) {
    if(APLX){std::clog << "[" << argv[0] << "]: No database '533/0-morton' found in " << CABINET_SFC << "." << std::endl;}
    else{std::clog << "[" << argv[0] << "]: No database '1030/2-morton' found in " << CABINET_SFC << "." << std::endl;}

    return retCode;
  }

  mdb_set_compare(txn, dbi, &compareMortonKeys);
  // Multiple values are stored by existing timeStamp in nanoseconds.
  mdb_set_dupsort(txn, dbi, &compareKeys);

  uint64_t numberOfEntries_SFC{0};

  if (!mdb_stat(txn, dbi, &stat)) {
    numberOfEntries_SFC = stat.ms_entries;
  }
  if(APLX){std::clog << "[" << argv[0] << "]: Found " << numberOfEntries_SFC << " entries in database '533/0-morton' in " << CABINET_SFC << std::endl;}
  else{std::clog << "[" << argv[0] << "]: Found " << numberOfEntries_SFC << " entries in database '1030/2-morton' in " << CABINET_SFC << std::endl;}
*/

float min_x = -10.0f;
float max_x = 10.0f;
float min_y = -6.0f;
float max_y = 6.0f;

////////////////////////////////////////////////////////////////////////////////

    _fenceBL.first = -1.5; _fenceBL.second = 0.0;
    _fenceTR.first = 0.75; _fenceTR.second = 5;
    DrivingStatus *leftCurve  = new DrivingStatus( "leftCurve",
            _fenceBL,
            _fenceTR,
            500000000,
            3000000000,
            -200000000,
            2000000000,
            50000000);

    _fenceBL.first = -1.5; _fenceBL.second = -5;
    _fenceTR.first = 0.75; _fenceTR.second = -0.0;
    DrivingStatus *rightCurve = new DrivingStatus( "rightCurve",
            _fenceBL,
            _fenceTR,
            500000000,
            3000000000,
            -200000000,
            2000000000,
            50000000);
    
    _fenceBL.first = 4; _fenceBL.second = -4;
    _fenceTR.first = 10; _fenceTR.second = 4;
    DrivingStatus *harsh_braking = new DrivingStatus( "harsh_braking",
            _fenceBL,
            _fenceTR,
            500000000,
            3000000000,
            -200000000,
            2000000000,
            50000000);

    std::vector<DrivingStatus*> maneuver;
    
    maneuver.push_back(leftCurve);
    maneuver.push_back(rightCurve);
    //maneuver.push_back(harsh_braking);

////////////////////////////////////////////////////////////////////////////////

    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::pair<int64_t, int64_t>> detection_BF = cabinet_queryManeuverBruteForce(MEM, CABINET, APLX, VERBOSE, _fenceBL, _fenceTR, maneuver);
    auto end = std::chrono::high_resolution_clock::now();
    int64_t duration_BF = std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count();
    
    start = std::chrono::high_resolution_clock::now();
    std::vector<std::pair<int64_t, int64_t>> detection_SFC = identifyManeuversSFC(argv, CABINET_SFC, MEM, VERBOSE, THR, APLX, geoboxStrings, geoboxBL, geoboxTR, maneuver);
    end = std::chrono::high_resolution_clock::now();
    int64_t duration_SFC = std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count();


    std::vector<std::pair<int64_t, int64_t>>false_negatives = getFalseNegatives(detection_BF, detection_SFC);
    std::vector<std::pair<int64_t, int64_t>>false_positives = getFalsePositives(detection_BF, detection_SFC);

////////////////////////////////////////////////////////////////////////////////
// Output all
    if(!detection_BF.empty()) {
      for(auto _temp : detection_BF) {
        std::cout << "BF:  Maneuver detected at: " << _temp.first << ", " << _temp.second << std::endl;
      }
    }
    if(!detection_SFC.empty()) {
      for(auto _temp : detection_SFC) {
        std::cout << "SFC: Maneuver detected at: " << _temp.first << ", " << _temp.second << std::endl;
      }
    }

    std::cout << "BF:  We detected " << detection_BF.size() << " Maneuvers" << std::endl;
    std::cout << "SFC: We detected " << detection_SFC.size() << " Maneuvers" << std::endl;

    std::cout << "Effectivity" << std::endl;

    if(detection_BF.size() != 0) {
      float detShare = (static_cast<float>(detection_BF.size())-static_cast<float>(false_negatives.size()))/static_cast<float>(detection_BF.size()) * 100.0f;
      std::cout << "(" << false_negatives.size() << " false negatives) : " << detection_BF.size()-false_negatives.size() << "/" << detection_BF.size() <<  " (" << detShare << "%)" <<" elements are detected by SFC-query" << std::endl;
    }
    else
      std::cout << "(" << false_negatives.size() << " false negatives) : " << detection_BF.size()-false_negatives.size() << "/" << detection_BF.size() << " elements are detected by SFC-query" << std::endl;

    std::cout << "(" << false_positives.size() << " false positives) : " << false_positives.size() << " elements are additionally detected by SFC-query." << std::endl;

    std::cout << "Efficiency" << std::endl;

    std::cout << "BF : " << duration_BF << " ms" << std::endl;
    std::cout << "SFC: " << duration_SFC << " ms" << std::endl;
  }
  return retCode;
}