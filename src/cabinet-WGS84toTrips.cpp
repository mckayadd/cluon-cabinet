/*
 * Copyright (C) 2022  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"
#include "cabinet-WGS84toTrips.hpp"
#include "geofence.hpp"

#include <array>
#include <iostream>
#include <sstream>
#include <string>

int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if ( (0 == commandlineArguments.count("cab")) || (0 == commandlineArguments.count("geofence")) ) {
    std::cerr << argv[0] << " traverse table 'all' of a cabinet (an lmdb-based key/value-database) to select only those WGS84 messages (19/?) that reside within a given geofence to be added to a Trips database." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --cab=myStore.cab --geofence=\"57.730744,12.159515;57.717822,12.189958\" [--out=myStore.cab-WGS84-Trips] [--mem=32024] [--verbose]" << std::endl;
    std::cerr << "         --cab:       name of the database file" << std::endl;
    std::cerr << "         --id:        id of the WGS84 coordinate to consider; default: 0" << std::endl;
    std::cerr << "         --out:       name of the database file to be created from the selected WGS84 locations codes" << std::endl;
    std::cerr << "         --mem:       upper memory size for database in memory in GB, default: 64,000 (representing 64TB)" << std::endl;
    std::cerr << "         --geofence:  polygon of WGS84 coordinates: coord1;coord2;coord3;...;coord_n; example: --geofence=\"57.730744,12.159515;57.717822,12.189958\"" << std::endl;
    std::cerr << "         --verbose:   display information on stderr" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cab=myStore.cab" << std::endl;
    retCode = 1;
  } else {
    const std::string CABINET{commandlineArguments["cab"]};
    const std::string TRIPSCABINET{(commandlineArguments["out"].size() != 0) ? commandlineArguments["out"] : CABINET + "-WGS84-Trips"};
    const uint64_t MEM{(commandlineArguments["mem"].size() != 0) ? static_cast<uint64_t>(std::stoi(commandlineArguments["mem"])) : 64UL*1024UL};
    const uint32_t ID{(commandlineArguments["id"].size() != 0) ? static_cast<uint32_t>(std::stoi(commandlineArguments["id"])) : 0};
    const bool VERBOSE{(commandlineArguments["verbose"].size() != 0)};
    const std::string geofence = commandlineArguments["geofence"];

    std::vector<std::string> listOfCoordinates = stringtoolbox::split(geofence, ';');
    std::vector<std::array<double,2>> polygon;

    for(auto c : listOfCoordinates) {
      std::stringstream sstr{c};
      double a, b;
      char d;
      sstr >> a >> d >> b;
      std::array<double,2> p{a, b};
      polygon.push_back(p);
    }

    if (VERBOSE) {
      std::cout << "Polygon for geofence: " << std::endl;
      for(auto c : polygon) {
        std::cout << c[0] << "," << c[1] << std::endl;
      }
    }

    retCode = cabinet_WGS84toTrips(MEM, CABINET, ID, polygon, TRIPSCABINET, VERBOSE);
  }
  return retCode;
}
