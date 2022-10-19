/*
 * Copyright (C) 2022  Lukas Birkemeyer
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"
#include "cabinet-query-maneuver.hpp"
//#include "DrivingStatus.hpp"

#include <cstdio>
#include <cstring>
#include <cstdint>

#include <iostream>
#include <iomanip>
#include <string>




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
            50000000);

    _fenceBL.first = -1.5; _fenceBL.second = -5;
    _fenceTR.first = 0.75; _fenceTR.second = -0.75;
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

    std::vector<std::pair<int64_t, int64_t>> detectionSFC = identifyManeuversSFC(argv, CABINET, MEM, VERBOSE, THR, APLX, geoboxStrings, geoboxBL, geoboxTR, maneuver);
  
    for(auto _temp : detectionSFC) {
      std::cout << "SFC: Maneuver detected at: " << _temp.first << ", " << _temp.second << std::endl;
    }
    std::cout << "SFC: We detected " << detectionSFC.size() << " Maneuvers" << std::endl;
  
  }
  return retCode;
}