/*
 * Copyright (C) 2022  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"
#include "cabinet-dump.hpp"

#include <iostream>
#include <sstream>
#include <string>

int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if (0 == commandlineArguments.count("cab")) {
    std::cerr << argv[0] << " traverse a table (default: 'all') of a cabinet (an lmdb-based key/value-database) to dump value to stdout." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --cab=myStore.cab [--out=myStore.cab-WGS84-Morton] [--mem=32024] [--verbose]" << std::endl;
    std::cerr << "         --cab:     name of the database file" << std::endl;
    std::cerr << "         --db:      name of the table to dump; default: all" << std::endl;
    std::cerr << "         --mem:     upper memory size for database in memory in GB, default: 64,000 (representing 64TB)" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cab=myStore.cab" << std::endl;
    retCode = 1;
  } else {
    const std::string CABINET{commandlineArguments["cab"]};
    const std::string DB{(commandlineArguments["db"].size() != 0) ? commandlineArguments["db"] : "all"};
    const uint64_t MEM{(commandlineArguments["mem"].size() != 0) ? static_cast<uint64_t>(std::stoi(commandlineArguments["mem"])) : 64UL*1024UL};

    retCode = cabinet_dump(MEM, CABINET, DB);
  }
  return retCode;
}
