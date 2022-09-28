/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"
#include "cabinet-acceltoMorton.hpp"

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
    std::cerr << "         --aplx:    whether Applenix data is required (e.g. Snowfox) or not (e.g. Voyager); default: not" << std::endl;
    std::cerr << "         --verbose: display information on stderr" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cab=myStore.cab" << std::endl;
    retCode = 1;
  } else {
    const std::string CABINET{commandlineArguments["cab"]};
    const std::string MORTONCABINET{(commandlineArguments["out"].size() != 0) ? commandlineArguments["out"] : CABINET + "-accel-Morton"};
    const uint64_t MEM{(commandlineArguments["mem"].size() != 0) ? static_cast<uint64_t>(std::stoi(commandlineArguments["mem"])) : 64UL*1024UL};
    const bool APLX{(commandlineArguments["aplx"].size() != 0)};
    const bool VERBOSE{(commandlineArguments["verbose"].size() != 0)};

    retCode = cabinet_acceltoMorton(MEM, CABINET, MORTONCABINET, APLX, VERBOSE);
  }
  return retCode;
}
