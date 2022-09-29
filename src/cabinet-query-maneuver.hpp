/*
 * Copyright (C) 2022  Lukas Birkemeyer
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef CABINETQUERYACCELFROMMORTON_HPP
#define CABINETQUERYACCELFROMMORTON_HPP

#include "cluon-complete.hpp"
#include "key.hpp"
#include "db.hpp"
#include "lmdb.h"
#include "morton.hpp"
#include "opendlv-standard-message-set.hpp"

#include <iostream>
#include <sstream>
#include <string>

inline int32_t identifyNonRelevantMortonBins(const std::pair<float,float> &BoxBL, const std::pair<float,float> &BoxTR, std::vector<uint64_t> * _nonRelevantMorton) {
  int32_t retCode{0};

  uint64_t bl_morton = 0;
  uint64_t tr_morton = 0;

  bl_morton = convertAccelLonTransToMorton(BoxBL);
  tr_morton = convertAccelLonTransToMorton(BoxTR);

  const uint32_t _xBL = std::lroundf((BoxBL.first + 10.0f) * 100);
  const uint32_t _yBL = std::lround((BoxBL.second + 10.0f) * 100);
  const uint32_t _xTR = std::lroundf((BoxTR.first + 10.0f) * 100);
  const uint32_t _yTR = std::lround((BoxTR.second + 10.0f) * 100);

  // add all values within morton ares to vector
  for (int i=bl_morton; i<= tr_morton; i++) {
    _nonRelevantMorton->push_back(i);
  }

  // remove morton values that are not covered by the fence -> only values that are not relevant remain
  int i, j;
  std::pair<float,float> _accelbox;
  uint64_t _morton;
  for (i=_yBL; i<_yTR; i++) {
    for (j=_xBL; j<_xTR; j++) {
      _accelbox.first = j/100.0f - 10.0f;
      _accelbox.second = i/100.0f - 10.0f;
      _morton = convertAccelLonTransToMorton(_accelbox);
      _nonRelevantMorton->erase(std::remove(_nonRelevantMorton->begin(), _nonRelevantMorton->end(), _morton), _nonRelevantMorton->end());
      //std::cout << _morton << std::endl;
    }
  }

  return retCode;
}

#endif