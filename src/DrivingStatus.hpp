/*
 * Copyright (C) 2022  Lukas Birkemeyer
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef DRIVINGSTATUS_HPP
#define DRIVINGSTATUS_HPP

#include "cluon-complete.hpp"
//#include "cabinet-query-maneuver.hpp"

#include <cstdio>
#include <cstring>
#include <cstdint>

#include <iostream>
#include <iomanip>
#include <string>


class DrivingStatus {
  public:
    char* name;
    std::pair<float,float> fenceBL;
    std::pair<float,float> fenceTR;
    uint64_t minDuration;
    uint64_t maxDuration;
    int64_t minGap; // to following;
    int64_t maxGap; // to following;
    int64_t minDiffTime; //how sensitive on single outliers
    std::vector<uint64_t> relevantMorton;
    std::vector<std::pair<int64_t,int64_t>> singleManeuverList;

    DrivingStatus(char *name,
                  std::pair<float,float> fenceBL,
                  std::pair<float,float> fenceTR,
                  uint64_t minDuration,
                  uint64_t maxDuration,
                  int64_t minGap,
                  int64_t maxGap,
                  int64_t minDiffTime)
                  : name(name),
                  fenceBL(fenceBL),
                  fenceTR(fenceTR),
                  minDuration(minDuration),
                  maxDuration(maxDuration),
                  minGap(minGap),
                  maxGap(maxGap),
                  minDiffTime(minDiffTime) {}
};


#endif