/*
 * Copyright (C) 2021,2022  Christian Berger
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
  if (0 == commandlineArguments.count("cab")) {
    std::cerr << argv[0] << " prints all entries from table 'all' of a cabinet (an lmdb-based key/value-database) as Envelopes to stdout." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --cab=myStore.cab [--mem=32024] [--verbose] [--start=1569916731] [--end=+100] [--export=19/0,25/1]" << std::endl;
    std::cerr << "         --cab:     name of the database file" << std::endl;
    std::cerr << "         --mem:     upper memory size for database in memory in GB, default: 64,000 (representing 64TB)" << std::endl;
    std::cerr << "         --verbose: display information on stderr" << std::endl;
    std::cerr << "         --start:   start time stamp in Unix epoch seconds (export begins AFTER this time point)" << std::endl;
    std::cerr << "         --end:     end time stamp in Unix epoch seconds; or +duration in seconds (export ends BEFORE this time point)" << std::endl;
    std::cerr << "         --export:  <list of messageID/senderStamp pairs to export, default: all>" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cab=myStore.cab" << std::endl;
    retCode = 1;
  } else {
    std::cerr.imbue(std::locale(std::cout.getloc(), new space_out));

    const std::string CABINET{commandlineArguments["cab"]};
    const uint64_t MEM{(commandlineArguments["mem"].size() != 0) ? static_cast<uint64_t>(std::stoi(commandlineArguments["mem"])) : 64UL*1024UL};
    const bool VERBOSE{(commandlineArguments["verbose"].size() != 0)};

    std::map<std::string, bool> mapOfEnvelopesToExport{};
    {
      std::string tmp{commandlineArguments["export"]};
      tmp += ",";
      auto entries = stringtoolbox::split(tmp, ',');
      for (auto e : entries) {
        // Use only valid entries.
        if (0 != e.size()) {
          auto l = stringtoolbox::split(e, '/');
          std::string toKeep{e};
          if (0 == l.size()) {
              toKeep += "/0";
          }
          std::cerr << argv[0] << " keeping " << toKeep << std::endl;
          mapOfEnvelopesToExport[toKeep] = true;
        }
      }
    }

    const int32_t START{(commandlineArguments.count("start") != 0) ? static_cast<int32_t>(std::stoi(commandlineArguments["start"])) : 0};
    int32_t END{(commandlineArguments.count("end") != 0) ? static_cast<int32_t>(std::stoi(commandlineArguments["end"])) : (std::numeric_limits<int32_t>::max)()};
    if (commandlineArguments.count("end") != 0) {
        std::string END_TMP = commandlineArguments["end"];
        if (END_TMP.at(0) == '+') {
          // relative end notation was used.
          END += START;
        }
    }

    const std::string ARGV0{argv[0]};
    retCode = cabinet_stream(ARGV0, MEM, CABINET, VERBOSE, mapOfEnvelopesToExport, START, END);
  }
  return retCode;
}
