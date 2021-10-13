/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "catch.hpp"

#include "morton.hpp"

#include <iostream>

TEST_CASE("Test encode/decode") {
	std::pair<std::uint32_t,std::uint32_t> xy1(57772400, 12765000);
	std::uint64_t mc1 = mortonEncode(xy1);
	std::pair<std::uint32_t,std::uint32_t> d1 = mortonDecode(mc1);
	//std::cout << xy1.first << " " << xy1.second << " " << mc1 << ", " << d1.first << "," << d1.second << std::endl;
  REQUIRE(xy1.first == d1.first);
  REQUIRE(xy1.second == d1.second);
  REQUIRE(mc1 == 1606428908008832);

	std::pair<std::uint32_t,std::uint32_t> xy2(57773800, 12772000);
	std::uint64_t mc2 = mortonEncode(xy2);
	std::pair<std::uint32_t,std::uint32_t> d2 = mortonDecode(mc2);
	//std::cout << xy2.first << " " << xy2.second << " " << mc2 << ", " << d2.first << ", " << d2.second << std::endl;
  REQUIRE(xy2.first == d2.first);
  REQUIRE(xy2.second == d2.second);
  REQUIRE(mc2 == 1606429041286208);
}
