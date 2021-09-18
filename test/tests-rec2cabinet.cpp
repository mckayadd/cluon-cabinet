/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifdef WIN32
    #define UNLINK _unlink
#else
    #include <unistd.h>
    #define UNLINK unlink
#endif

#include "catch.hpp"
#include "rec2cabinet.hpp"
#include "cabinet2rec.hpp"

#include <fstream>
#include <string>

TEST_CASE("Test rec2cabinet") {
  const unsigned char recfile[] = {
    0x0d, 0xa4, 0x3c, 0x00, 0x00, 0x08, 0x26, 0x12, 0x12, 0x09, 0x6c, 0x26,
    0x20, 0x6c, 0x00, 0xe3, 0x4c, 0x40, 0x19, 0xd0, 0xfc, 0xe2, 0x8f, 0x66,
    0x88, 0x29, 0x40, 0x1a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10,
    0xce, 0x8b, 0x66, 0x22, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10,
    0xe8, 0x8b, 0x66, 0x2a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10,
    0x8e, 0xb4, 0x65, 0x30, 0x00, 0x0d, 0xa4, 0x3c, 0x00, 0x00, 0x08, 0x26,
    0x12, 0x12, 0x09, 0xdc, 0x0d, 0x1f, 0x6c, 0x00, 0xe3, 0x4c, 0x40, 0x19,
    0x1b, 0x79, 0x3f, 0x90, 0x66, 0x88, 0x29, 0x40, 0x1a, 0x0a, 0x08, 0xa2,
    0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xac, 0xa7, 0x67, 0x22, 0x0a, 0x08, 0xa2,
    0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xc2, 0xa7, 0x67, 0x2a, 0x0a, 0x08, 0xa2,
    0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xae, 0xd0, 0x66, 0x30, 0x00, 0x0d, 0xa4,
    0x3c, 0x00, 0x00, 0x08, 0x26, 0x12, 0x12, 0x09, 0xc8, 0x70, 0x1d, 0x6c,
    0x00, 0xe3, 0x4c, 0x40, 0x19, 0xbe, 0x31, 0x99, 0x90, 0x66, 0x88, 0x29,
    0x40, 0x1a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xea, 0xc4,
    0x68, 0x22, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0x8c, 0xc5,
    0x68, 0x2a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xce, 0xec,
    0x67, 0x30, 0x00, 0x0d, 0xa4, 0x3c, 0x00, 0x00, 0x08, 0x26, 0x12, 0x12,
    0x09, 0x2c, 0xaf, 0x5d, 0x57, 0x00, 0xe3, 0x4c, 0x40, 0x19, 0xc1, 0x7f,
    0x89, 0x60, 0x67, 0x88, 0x29, 0x40, 0x1a, 0x0a, 0x08, 0xa2, 0xce, 0xa5,
    0xc9, 0x0b, 0x10, 0x8e, 0xb2, 0x68, 0x22, 0x0a, 0x08, 0xa2, 0xce, 0xa5,
    0xc9, 0x0b, 0x10, 0xbe, 0xb2, 0x68, 0x2a, 0x0a, 0x08, 0xa2, 0xce, 0xa5,
    0xc9, 0x0b, 0x10, 0xc8, 0xb1, 0x68, 0x30, 0x01, 0x0d, 0xa4, 0x3c, 0x00,
    0x00, 0x08, 0x26, 0x12, 0x12, 0x09, 0xbf, 0x1a, 0x1c, 0x6c, 0x00, 0xe3,
    0x4c, 0x40, 0x19, 0x1b, 0x16, 0xf3, 0x90, 0x66, 0x88, 0x29, 0x40, 0x1a,
    0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0x88, 0xe2, 0x69, 0x22,
    0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xb4, 0xe2, 0x69, 0x2a,
    0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xee, 0x88, 0x69, 0x30,
    0x00, 0x0d, 0xa4, 0x3c, 0x00, 0x00, 0x08, 0x26, 0x12, 0x12, 0x09, 0x08,
    0x03, 0x1b, 0x6c, 0x00, 0xe3, 0x4c, 0x40, 0x19, 0x02, 0x0c, 0x50, 0x91,
    0x66, 0x88, 0x29, 0x40, 0x1a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b,
    0x10, 0xae, 0xfe, 0x6a, 0x22, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b,
    0x10, 0xdc, 0xfe, 0x6a, 0x2a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b,
    0x10, 0x8e, 0xa5, 0x6a, 0x30, 0x00, 0x0d, 0xa4, 0x3e, 0x00, 0x00, 0x08,
    0x92, 0x1c, 0x12, 0x13, 0x0d, 0x66, 0x66, 0x9c, 0xc1, 0x15, 0x14, 0xae,
    0xbb, 0xc1, 0x1d, 0x33, 0x43, 0x79, 0x44, 0x20, 0x03, 0x28, 0x00, 0x1a,
    0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xca, 0xe8, 0x6a, 0x22,
    0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0x94, 0xa7, 0x6b, 0x2a,
    0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xa2, 0xe6, 0x6a, 0x30,
    0x02, 0x0d, 0xa4, 0x3a, 0x00, 0x00, 0x08, 0x8c, 0x10, 0x12, 0x0f, 0x0d,
    0x66, 0x66, 0x9c, 0xc1, 0x15, 0x14, 0xae, 0xbb, 0xc1, 0x1d, 0x33, 0x43,
    0x79, 0x44, 0x1a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xdc,
    0xe9, 0x6a, 0x22, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0x82,
    0xa8, 0x6b, 0x2a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xa2,
    0xe6, 0x6a, 0x30, 0x02, 0x0d, 0xa4, 0x3c, 0x00, 0x00, 0x08, 0x26, 0x12,
    0x12, 0x09, 0xf3, 0xab, 0x19, 0x6c, 0x00, 0xe3, 0x4c, 0x40, 0x19, 0xb1,
    0xb7, 0xab, 0x91, 0x66, 0x88, 0x29, 0x40, 0x1a, 0x0a, 0x08, 0xa2, 0xce,
    0xa5, 0xc9, 0x0b, 0x10, 0xfc, 0x99, 0x6c, 0x22, 0x0a, 0x08, 0xa2, 0xce,
    0xa5, 0xc9, 0x0b, 0x10, 0xaa, 0x9a, 0x6c, 0x2a, 0x0a, 0x08, 0xa2, 0xce,
    0xa5, 0xc9, 0x0b, 0x10, 0xae, 0xc1, 0x6b, 0x30, 0x00, 0x0d, 0xa4, 0x3c,
    0x00, 0x00, 0x08, 0x26, 0x12, 0x12, 0x09, 0x78, 0x26, 0x18, 0x6c, 0x00,
    0xe3, 0x4c, 0x40, 0x19, 0xc4, 0x96, 0x05, 0x92, 0x66, 0x88, 0x29, 0x40,
    0x1a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0x80, 0xb8, 0x6d,
    0x22, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xb8, 0xb8, 0x6d,
    0x2a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xce, 0xdd, 0x6c,
    0x30, 0x00, 0x0d, 0xa4, 0x3c, 0x00, 0x00, 0x08, 0x26, 0x12, 0x12, 0x09,
    0x00, 0x00, 0x00, 0xc0, 0x00, 0xe3, 0x4c, 0x40, 0x19, 0x00, 0x00, 0x00,
    0xe0, 0x6c, 0x88, 0x29, 0x40, 0x1a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9,
    0x0b, 0x10, 0x8e, 0xe6, 0x6d, 0x22, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9,
    0x0b, 0x10, 0xc2, 0x8e, 0x70, 0x2a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9,
    0x0b, 0x10, 0xa8, 0xe5, 0x6d, 0x30, 0x02, 0x0d, 0xa4, 0x3c, 0x00, 0x00,
    0x08, 0x26, 0x12, 0x12, 0x09, 0x2b, 0xa7, 0x16, 0x6c, 0x00, 0xe3, 0x4c,
    0x40, 0x19, 0x89, 0x6c, 0x61, 0x92, 0x66, 0x88, 0x29, 0x40, 0x1a, 0x0a,
    0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xde, 0xd3, 0x6e, 0x22, 0x0a,
    0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0x96, 0xd4, 0x6e, 0x2a, 0x0a,
    0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xee, 0xf9, 0x6d, 0x30, 0x00,
    0x0d, 0xa4, 0x3c, 0x00, 0x00, 0x08, 0x26, 0x12, 0x12, 0x09, 0x86, 0xbc,
    0x54, 0x57, 0x00, 0xe3, 0x4c, 0x40, 0x19, 0x69, 0xc9, 0x05, 0x60, 0x67,
    0x88, 0x29, 0x40, 0x1a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10,
    0xbc, 0xc0, 0x6e, 0x22, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10,
    0x88, 0xc1, 0x6e, 0x2a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10,
    0xd8, 0xbf, 0x6e, 0x30, 0x01, 0x0d, 0xa4, 0x3c, 0x00, 0x00, 0x08, 0x26,
    0x12, 0x12, 0x09, 0xbc, 0x62, 0x15, 0x6c, 0x00, 0xe3, 0x4c, 0x40, 0x19,
    0xdb, 0xce, 0xbe, 0x92, 0x66, 0x88, 0x29, 0x40, 0x1a, 0x0a, 0x08, 0xa2,
    0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xba, 0xef, 0x6f, 0x22, 0x0a, 0x08, 0xa2,
    0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xf2, 0xef, 0x6f, 0x2a, 0x0a, 0x08, 0xa2,
    0xce, 0xa5, 0xc9, 0x0b, 0x10, 0x8e, 0x96, 0x6f, 0x30, 0x00, 0x0d, 0xa4,
    0x3c, 0x00, 0x00, 0x08, 0x26, 0x12, 0x12, 0x09, 0xdb, 0xbe, 0x13, 0x6c,
    0x00, 0xe3, 0x4c, 0x40, 0x19, 0xb7, 0x47, 0x19, 0x93, 0x66, 0x88, 0x29,
    0x40, 0x1a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xee, 0x8b,
    0x71, 0x22, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xa6, 0x8c,
    0x71, 0x2a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xae, 0xb2,
    0x70, 0x30, 0x00, 0x0d, 0xa4, 0x3c, 0x00, 0x00, 0x08, 0x26, 0x12, 0x12,
    0x09, 0xb7, 0x5e, 0x12, 0x6c, 0x00, 0xe3, 0x4c, 0x40, 0x19, 0xad, 0x06,
    0x75, 0x93, 0x66, 0x88, 0x29, 0x40, 0x1a, 0x0a, 0x08, 0xa2, 0xce, 0xa5,
    0xc9, 0x0b, 0x10, 0xe6, 0xa7, 0x72, 0x22, 0x0a, 0x08, 0xa2, 0xce, 0xa5,
    0xc9, 0x0b, 0x10, 0x9e, 0xa8, 0x72, 0x2a, 0x0a, 0x08, 0xa2, 0xce, 0xa5,
    0xc9, 0x0b, 0x10, 0xce, 0xce, 0x71, 0x30, 0x00, 0x0d, 0xa4, 0x3c, 0x00,
    0x00, 0x08, 0x26, 0x12, 0x12, 0x09, 0xd4, 0x26, 0x11, 0x6c, 0x00, 0xe3,
    0x4c, 0x40, 0x19, 0xe7, 0x64, 0xd3, 0x93, 0x66, 0x88, 0x29, 0x40, 0x1a,
    0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xfa, 0xc3, 0x73, 0x22,
    0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xba, 0xc4, 0x73, 0x2a,
    0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xee, 0xea, 0x72, 0x30,
    0x00, 0x0d, 0xa4, 0x3c, 0x00, 0x00, 0x08, 0x26, 0x12, 0x12, 0x09, 0xf7,
    0x8f, 0x0f, 0x6c, 0x00, 0xe3, 0x4c, 0x40, 0x19, 0x32, 0xbd, 0x2f, 0x94,
    0x66, 0x88, 0x29, 0x40, 0x1a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b,
    0x10, 0x80, 0xe1, 0x74, 0x22, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b,
    0x10, 0xb8, 0xe1, 0x74, 0x2a, 0x0a, 0x08, 0xa2, 0xce, 0xa5, 0xc9, 0x0b,
    0x10, 0x90, 0x87, 0x74, 0x30, 0x00, 0x0d, 0xa4, 0x3c, 0x00, 0x00, 0x08,
    0x26, 0x12, 0x12, 0x09, 0x48, 0xc5, 0x85, 0x57, 0x00, 0xe3, 0x4c, 0x40,
    0x19, 0x1f, 0xd8, 0x9b, 0x5f, 0x67, 0x88, 0x29, 0x40, 0x1a, 0x0a, 0x08,
    0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xf8, 0xce, 0x74, 0x22, 0x0a, 0x08,
    0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0xc2, 0xcf, 0x74, 0x2a, 0x0a, 0x08,
    0xa2, 0xce, 0xa5, 0xc9, 0x0b, 0x10, 0x94, 0xce, 0x74, 0x30, 0x01
  };
  unsigned int recfile_len = 1235;

  const bool VERBOSE{true};
  const std::string RECFILENAME{"tests-rec2cabinet.rec"};
  const std::string CABINETNAME{"tests-rec2cabinet.cab"};
  const std::string CABINETNAME_LOCK{"tests-rec2cabinet.cab-lock"};
  const std::string REC2FILENAME{"tests-rec2cabinet.rec2"};
  UNLINK(RECFILENAME.c_str());
  UNLINK(CABINETNAME.c_str());
  UNLINK(CABINETNAME_LOCK.c_str());
  UNLINK(REC2FILENAME.c_str());
  {
    std::fstream rec(RECFILENAME.c_str(), std::ios::out|std::ios::binary|std::ios::trunc);
    rec.write(reinterpret_cast<const char*>(recfile), recfile_len);
    rec.flush();
    rec.close();
  }
  REQUIRE(0 == rec2cabinet("tests-rec2cabinet", RECFILENAME, CABINETNAME, VERBOSE));
  UNLINK(RECFILENAME.c_str());

  REQUIRE(0 == cabinet2rec("tests-rec2cabinet", CABINETNAME, REC2FILENAME, VERBOSE));
  {
    std::fstream fin{REC2FILENAME.c_str(), std::ios::in|std::ios::binary};
    if (fin.good()) {
      const std::string s{static_cast<std::stringstream const&>(std::stringstream() << fin.rdbuf()).str()};
      REQUIRE(s.size() == recfile_len);
      REQUIRE(0 == strncmp(reinterpret_cast<const char*>(recfile), s.c_str(), recfile_len));
    }
  }
  UNLINK(CABINETNAME.c_str());
  UNLINK(CABINETNAME_LOCK.c_str());
  UNLINK(REC2FILENAME.c_str());
}
