/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "cluon-complete.hpp"

#include "lmdb.h"

#include <iostream>
#include <locale>
#include <sstream>

struct space_out : std::numpunct<char> {
  char do_thousands_sep()   const { return ','; }  // separate with spaces
  std::string do_grouping() const { return "\3"; } // groups of 3 digit
};

int32_t main(int32_t argc, char **argv) {
  int32_t retCode{0};
  auto commandlineArguments = cluon::getCommandlineArguments(argc, argv);
  if (0 == commandlineArguments.count("cab")) {
    std::cerr << argv[0] << " display contents of a .cab file with envelopes." << std::endl;
    std::cerr << "Usage:   " << argv[0] << " --cab=MyFile.cab [--odvd=MyFile.odvd] [--mem=32024]" << std::endl;
    std::cerr << "         --cab:      name of the database file" << std::endl;
    std::cerr << "         --odvd:     name of the ODVD message specification to display resolvable envelopes" << std::endl;
    std::cerr << "         --mem:      upper memory size for database in memory in GB, default: 64,000 (representing 64TB)" << std::endl;
    std::cerr << "Example: " << argv[0] << " --cab=MyFile.cab --odvd=MySpec.odvd" << std::endl;
    retCode = 1;
  }
  else {
    std::pair<std::vector<cluon::MetaMessage>, cluon::MessageParser::MessageParserErrorCodes> messageParserResult;
    {
      cluon::MessageParser mp;
      std::ifstream fin(commandlineArguments["odvd"], std::ios::in);
      if (fin.good()) {
        std::string input(static_cast<std::stringstream const&>(std::stringstream() << fin.rdbuf()).str());
        fin.close();
        messageParserResult = mp.parse(input);
        std::clog << "[" << argv[0] << "]: Found " << messageParserResult.first.size() << " message(s) in " << commandlineArguments["odvd"] << std::endl;
      }
      else {
        std::cerr << argv[0] << ": Message specification '" << commandlineArguments["odvd"] << "' not found." << std::endl;
      }
    }
    std::map<int32_t, cluon::MetaMessage> scope;
    for (const auto &e : messageParserResult.first) { scope[e.messageIdentifier()] = e; }

    const std::string CAB{commandlineArguments["cab"]};
    const uint64_t MEM{(commandlineArguments["mem"].size() != 0) ? static_cast<uint64_t>(std::stoi(commandlineArguments["mem"])) : 64UL*1024UL};

    // lambda to check the interaction with the database.
    auto checkErrorCode = [_argv=argv](int32_t rc, int32_t line, std::string caller) {
      if (MDB_SUCCESS != rc) {
        std::cerr << "[" << _argv[0] << "]: " << caller << ", line " << line << ": (" << rc << ") " << mdb_strerror(rc) << std::endl; 
      }
      return (MDB_SUCCESS == rc);
    };

    MDB_env *env{nullptr};

    if (!checkErrorCode(mdb_env_create(&env), __LINE__, "mdb_env_create")) {
      return (retCode = 1);
    }
    int numberOfDatabases{100};

    if (!checkErrorCode(mdb_env_set_maxdbs(env, numberOfDatabases), __LINE__, "mdb_env_set_maxdbs")) {
      mdb_env_close(env);
      return (retCode = 1);
    }
    const int64_t SIZE_DB = MEM * 1024UL * 1024UL * 1024UL;
    if (!checkErrorCode(mdb_env_set_mapsize(env, SIZE_DB), __LINE__, "mdb_env_set_mapsize")) {
      mdb_env_close(env);
      return (retCode = 1);
    }
    if (!checkErrorCode(mdb_env_open(env, CAB.c_str(), MDB_NOSUBDIR|MDB_RDONLY, 0600), __LINE__, "mdb_env_open")) {
      mdb_env_close(env);
      return (retCode = 1);
    }

    {
      MDB_txn *txn{nullptr};
      MDB_dbi dbi{0};
      if (!checkErrorCode(mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn), __LINE__, "mdb_txn_begin")) {
        mdb_env_close(env);
        return (retCode = 1);
      }
      retCode = mdb_dbi_open(txn, nullptr, 0 , &dbi);
      if (MDB_NOTFOUND  == retCode) {
        std::clog << "[" << argv[0] << "]: No database nullptr found in " << CAB << ", will be created on opening." << std::endl;
      }
      else {
        uint64_t numberOfEntries{0};
        std::clog.imbue(std::locale(std::cout.getloc(), new space_out));
        MDB_stat stat;
        {
          MDB_dbi dbAll{0};
          retCode = mdb_dbi_open(txn, "all", 0 , &dbAll);
          if (!mdb_stat(txn, dbAll, &stat)) {
            numberOfEntries = stat.ms_entries;
          }
          std::clog << "[" << argv[0] << "]: 'all': " << numberOfEntries << " entries" << std::endl;
          mdb_close(env, dbAll);
        }

        MDB_cursor *cursor;
        if (!(retCode = mdb_cursor_open(txn, dbi, &cursor))) {
          int rc{0};
          MDB_val key;
          MDB_stat mst;
          while ((retCode = mdb_cursor_get(cursor, &key, nullptr, MDB_NEXT_NODUP)) == 0) {
            // Scan key for dateType/senderStamp name.
            if (memchr(key.mv_data, '\0', key.mv_size)) {
              continue;
            }
            std::string s(static_cast<char*>(key.mv_data), key.mv_size);

            MDB_dbi db2;
            rc = mdb_open(txn, s.c_str(), 0, &db2);
            if (rc) {
              continue;
            }
            rc = mdb_stat(txn, db2, &mst);
            if (!rc) {
              auto type = stringtoolbox::split(s, '/');
              if (type.size() == 2) {
                std::string dataTypeName = stringtoolbox::split(s, '/')[0];
                std::string senderStamp = stringtoolbox::split(s, '/')[1];
                std::string name = "unknown";
                int dtn{0};
                try {
                  dtn = std::stoi(dataTypeName);
                }
                catch(...){}
                if (scope.count(dtn) > 0) {
                  cluon::MetaMessage m = scope[std::stoi(dataTypeName)];
                  name = m.messageName();
                }
                std::clog << "[" << argv[0] << "]: " << dataTypeName << "/" << senderStamp << " ('" << name << "'): " << mst.ms_entries << " entries" << std::endl;
              }
            }
            mdb_close(env, db2);
          }
          mdb_cursor_close(cursor);
        }
      }
      mdb_txn_abort(txn);
      if (dbi) {
        mdb_dbi_close(env, dbi);
      }
    }
    if (env) {
      mdb_env_close(env);
    }
  }
  return retCode;
}
