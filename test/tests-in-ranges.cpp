/*
 * Copyright (C) 2022  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "catch.hpp"

#include "in-ranges.hpp"

TEST_CASE("Test empty ranges") {
  cluon::In_Ranges<uint32_t> IR;

  REQUIRE(!IR.isInAnyRange(1));
  REQUIRE(!IR.isInAnyRange(2));
  REQUIRE(!IR.isInAnyRange(7));
  REQUIRE(!IR.isInAnyRange(10));
}

TEST_CASE("Test ranges") {
  cluon::In_Ranges<uint32_t> IR;
  IR.addRange(std::make_pair(2, 4));
  IR.addRange(std::make_pair(6, 7));
  IR.addRange(std::make_pair(9, 12));

  REQUIRE(!IR.isInAnyRange(1));
  REQUIRE(IR.isInAnyRange(2));
  REQUIRE(IR.isInAnyRange(7));
  REQUIRE(IR.isInAnyRange(10));
}

TEST_CASE("Test epochs") {
  cluon::In_Ranges<uint64_t> IR;
  IR.addRange(std::make_pair(1608099630322516000, 1608104277224115000));
  IR.addRange(std::make_pair(1608108723409431000, 1608111802194196000));
  IR.addRange(std::make_pair(1608116930379220000, 1608120077139878000));
  IR.addRange(std::make_pair(1608178024311792000, 1608181062148791000));

  REQUIRE(!IR.isInAnyRange(1));
  REQUIRE(IR.isInAnyRange(1608099630322516000));
  REQUIRE(IR.isInAnyRange(1608099640322516000));
  REQUIRE(IR.isInAnyRange(1608104277224115000));
  REQUIRE(!IR.isInAnyRange(1608104277224115001));
  REQUIRE(IR.isInAnyRange(1608178054311792000));
}
