/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "catch.hpp"

#include "cluon-complete.hpp"
#include "key.hpp"

#include <cstring>
#include <iostream>
#include <iomanip>
#include <vector>

TEST_CASE("Test writing key") {
  std::vector<char> tmp;
  tmp.reserve(511);

  cabinet::Key k;
  k.timeStamp(12345)
	 .dataType(4321)
	 .senderStamp(223344)
	 .hash(987654321)
	 .hashOfRecFile(1219289910)
   .length(345)
	 .version(0);

  const size_t len = setKey(k, tmp.data(), tmp.capacity());
  REQUIRE(31 == len);

  const unsigned char output[] = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x10, 0xe1, 0x00, 0x03, 0x68, 0x70, 0x00, 0x00, 0x00, 0x00, 0x3a, 0xde, 0x68, 0xb1, 0x48, 0xac, 0xe3, 0x36, 0x01, 0x59, 0x00 };

  REQUIRE(0 == strncmp(tmp.data(), reinterpret_cast<const char*>(output), len));

//  std::cerr << std::hex << std::setw(2) << std::setfill('0');
//  for(size_t i{0}; i < len; i++) {
//    std::cerr << "0x" << std::hex << std::setw(2) << std::setfill('0') << +static_cast<uint8_t>(tmp.data()[i]) << ", ";
//  }
//  std::cerr << std::dec << std::endl;
}

TEST_CASE("Test reading key") {
  std::vector<char> tmp;
  tmp.reserve(511);

  const unsigned char input[] = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x39, 0x00, 0x00, 0x10, 0xe1, 0x00, 0x03, 0x68, 0x70, 0x00, 0x00, 0x00, 0x00, 0x3a, 0xde, 0x68, 0xb1, 0x48, 0xac, 0xe3, 0x36, 0x01, 0x59, 0x00 };
  const size_t length{31};
  std::memcpy(tmp.data(), input, length);

  cabinet::Key k;
  REQUIRE(0 == k.timeStamp());
  REQUIRE(0 == k.dataType());
  REQUIRE(0 == k.senderStamp());
  REQUIRE(0 == k.hash());
  REQUIRE(0 == k.hashOfRecFile());
  REQUIRE(0 == k.length());
  REQUIRE(0 == k.version());

  k = getKey(tmp.data(), tmp.capacity());

  REQUIRE(12345 == k.timeStamp());
  REQUIRE(4321 == k.dataType());
  REQUIRE(223344 == k.senderStamp());
  REQUIRE(987654321 == k.hash());
  REQUIRE(1219289910 == k.hashOfRecFile());
  REQUIRE(345 == k.length());
  REQUIRE(0 == k.version());
}
