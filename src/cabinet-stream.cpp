/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"
#include "cabinet-stream.hpp"

#include <iostream>
#include <iomanip>
#include <string>

struct space_out : std::numpunct<char> {
  char do_thousands_sep()   const { return ','; }  // separate with spaces
  std::string do_grouping() const { return "\3"; } // groups of 3 digit
};

int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if (0 == commandlineArguments.count("cabinet")) {
    std::cerr << argv[0] << " prints all entries from table 'all' of a cabinet (an lmdb-based key/value-database) as Envelopes to stdout." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --cabinet=myStore.cabinet" << std::endl;
    std::cerr << "         --cabinet: name of the database file" << std::endl;
    std::cerr << "         --verbose: display information on stderr" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cabinet=myStore.cabinet" << std::endl;
    retCode = 1;
  } else {
    std::cerr.imbue(std::locale(std::cout.getloc(), new space_out));

    const std::string CABINET{commandlineArguments["cabinet"]};
    const bool VERBOSE{(commandlineArguments["verbose"].size() != 0)};

    const std::string ARGV0{argv[0]};
    retCode = cabinet_stream(ARGV0, CABINET, VERBOSE);
  }
  return retCode;
}
