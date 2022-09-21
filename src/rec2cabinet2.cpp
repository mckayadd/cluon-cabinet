/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"
#include "rec2cabinet2.hpp"

#include <fstream>
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
    std::cerr << "Usage:   " << argv[0] << " --rec=MyFile.rec [--verbose] [--cab=myFile.cab] [--mem=32024] [--userdata=1234] [--temporalrange=times.csv]" << std::endl;
    std::cerr << "         --rec:            name of the recording file" << std::endl;
    std::cerr << "         --cab:            name of the database file (optional; otherwise, a new file based on the .rec file with .cab as suffix is created)" << std::endl;
    std::cerr << "         --mem:            upper memory size for database in memory in GB, default: 64,000 (representing 64TB)" << std::endl;
    std::cerr << "         --userdata:       optional: uint64_t user supplied optional data (default: 0), which can be used to add further information to this import" << std::endl;
    std::cerr << "         --temporalranges: optional: csv file (format: start-timestamp;end-timestamp) to specify, in which temporal range a data sample to add must reside" << std::endl;
    std::cerr << "         --verbose:        display information" << std::endl;
    std::cerr << "Example: " << argv[0] << " --rec=myFile.rec --cab=myStore.cab --mem=64000" << std::endl;
    retCode = 1;
  } else {
    std::clog.imbue(std::locale(std::cout.getloc(), new space_out));
    const std::string REC{commandlineArguments["rec"]};
    const std::string CABINET{(commandlineArguments["cab"].size() != 0) ? commandlineArguments["cab"] : "./" + REC + ".cab"};
    const uint64_t MEM{(commandlineArguments["mem"].size() != 0) ? static_cast<uint64_t>(std::stoi(commandlineArguments["mem"])) : 64UL*1024UL};
    const uint64_t USERDATA{(commandlineArguments["userdata"].size() != 0) ? static_cast<uint64_t>(std::stoll(commandlineArguments["userdata"])) : 0};
    const std::string TEMPORAL_RANGES{(commandlineArguments["temporalranges"].size() != 0) ? commandlineArguments["temporalranges"] : ""};
    const bool VERBOSE{(commandlineArguments["verbose"].size() != 0)};

    cluon::In_Ranges<int64_t> ranges;
    {
      std::fstream CSVtemporalRanges(TEMPORAL_RANGES.c_str(), std::ios::in);
      if (CSVtemporalRanges.good()) {
        for(std::string line; std::getline(CSVtemporalRanges, line);) {
          std::stringstream sstr(line);
          int64_t start{0},end{0};
          char delim{';'};
          sstr >> start >> delim >> end;
          ranges.addRange(std::make_pair(start, end));
        }
      }
    }

    const std::string ARGV0{argv[0]};
    retCode = rec2cabinet(ARGV0, MEM, REC, CABINET, USERDATA, ranges, VERBOSE);
  }
  return retCode;
}
