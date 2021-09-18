/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"
#include "rec2cabinet.hpp"

#include <iostream>
#include <iomanip>
#include <locale>
#include <string>

struct space_out : std::numpunct<char> {
  char do_thousands_sep()   const { return ','; }  // separate with spaces
  std::string do_grouping() const { return "\3"; } // groups of 3 digit
};

int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if (0 == commandlineArguments.count("rec")) {
    std::cerr << argv[0] << " transforms a .rec file with Envelopes to an lmdb-based key/value-database." << std::endl;
    std::cerr << "If the specified database exists, the content of the .rec file is added." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --rec=MyFile.rec [--verbose] [--cabinet=myFile.cab]" << std::endl;
    std::cerr << "         --rec:     name of the recording file" << std::endl;
    std::cerr << "         --cabinet: name of the database file (optional; otherwise, a new file based on the .rec file with .cabinet as suffix is created)" << std::endl;
    std::cerr << "         --verbose: display information" << std::endl;
    std::cerr << "Example: " << argv[0] << " --rec=myFile.rec --cabinet=myStore.cab" << std::endl;
    retCode = 1;
  } else {
    std::clog.imbue(std::locale(std::cout.getloc(), new space_out));
    const std::string REC{commandlineArguments["rec"]};
    const std::string CABINET{(commandlineArguments["cabinet"].size() != 0) ? commandlineArguments["cabinet"] : "./" + REC + ".cabinet"};
    const bool VERBOSE{(commandlineArguments["verbose"].size() != 0)};

    const std::string ARGV0{argv[0]};
    retCode = rec2cabinet(ARGV0, REC, CABINET, VERBOSE);
  }
  return retCode;
}
