/*
 * Copyright (C) 2022  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "catch.hpp"

#include "in-ranges.hpp"

TEST_CASE("Test writing key") {
  cluon::In_Ranges<uint32_t> IR;
  IR.addRange(std::make_pair(2, 4));
  IR.addRange(std::make_pair(6, 7));
  IR.addRange(std::make_pair(9, 12));

  REQUIRE(!IR.isInAnyRange(1));
  REQUIRE(IR.isInAnyRange(2));
  REQUIRE(IR.isInAnyRange(7));
  REQUIRE(IR.isInAnyRange(10));
}
