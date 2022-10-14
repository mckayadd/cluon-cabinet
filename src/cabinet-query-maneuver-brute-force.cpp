/*
 * Copyright (C) 2022  Lukas Birkemeyer
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"
#include "cabinet-query-maneuver-brute-force.hpp"

#include <iostream>
#include <sstream>
#include <string>

int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if (0 == commandlineArguments.count("cab")) {
    std::cerr << argv[0] << " traverse table 'all' of a cabinet (an lmdb-based key/value-database) to convert Acceleration messages (1030/?) to Morton index." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --cab=myStore.cab [--out=myStore.cab-accel-Morton] [--mem=32024] [--verbose]" << std::endl;
    std::cerr << "         --cab:     name of the database file" << std::endl;
    std::cerr << "         --out:     name of the database file to be created from the converted Morton codes" << std::endl;
    std::cerr << "         --mem:     upper memory size for database in memory in GB, default: 64,000 (representing 64TB)" << std::endl;
    std::cerr << "         --geobox: return all timeStamps for GPS locations within this rectangle specified by bottom-left and top-right lat/longs" << std::endl;
    std::cerr << "         --aplx:    Applenix data required (e.g. Snowfox)? default: no (e.g. Voyager)" << std::endl;
    std::cerr << "         --verbose: display information on stderr" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cab=myStore.cab" << std::endl;
    retCode = 1;
  } else {
    const std::string CABINET{commandlineArguments["cab"]};
    const std::string MORTONCABINET{(commandlineArguments["out"].size() != 0) ? commandlineArguments["out"] : CABINET + "-accel-Morton"};
    const uint64_t MEM{(commandlineArguments["mem"].size() != 0) ? static_cast<uint64_t>(std::stoi(commandlineArguments["mem"])) : 64UL*1024UL};
    const std::string GEOBOX{commandlineArguments["geobox"]};
    const bool APLX{(commandlineArguments["aplx"].size() != 0)};
    const bool VERBOSE{(commandlineArguments["verbose"].size() != 0)};
    std::vector<std::string> geoboxStrings = stringtoolbox::split(GEOBOX, ',');
    std::pair<float,float> _fenceBL;
    std::pair<float,float> _fenceTR;
    if (4 == geoboxStrings.size()) {
      _fenceBL.first = std::stof(geoboxStrings.at(0));
      _fenceBL.second = std::stof(geoboxStrings.at(1));
      _fenceTR.first = std::stof(geoboxStrings.at(2));
      _fenceTR.second = std::stof(geoboxStrings.at(3));
    }

    //////////////////////////////////////////////////////////////

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

    retCode = cabinet_queryManeuverBruteForce(MEM, CABINET, MORTONCABINET, APLX, VERBOSE, _fenceBL, _fenceTR, maneuver);
  }
  return retCode;
}
