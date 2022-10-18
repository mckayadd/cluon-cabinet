/*
 * Copyright (C) 2022  Lukas Birkemeyer
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef CABINETSFCQUERYEXPERIMENT_HPP
#define CABINETSFCQUERYEXPERIMENT_HPP

#include "cluon-complete.hpp"
#include "key.hpp"
#include "db.hpp"
#include "lmdb.h"
#include "morton.hpp"
#include "opendlv-standard-message-set.hpp"
#include "cabinet-query-maneuver-brute-force.hpp"
#include "cabinet-query-maneuver.hpp"

#include <iostream>
#include <sstream>
#include <string>


inline std::vector<std::pair<int64_t, int64_t>> getFalseNegatives(std::vector<std::pair<int64_t, int64_t>> detection_BF, std::vector<std::pair<int64_t, int64_t>> detection_SFC) {

    std::vector<std::pair<int64_t, int64_t>> false_negatives;

    //check, whether all elements of BF are also detected by morton (true positives and false positives)
    for(std::pair<int64_t, int64_t> temp : detection_BF){
        if(std::find(detection_SFC.begin(), detection_SFC.end(), temp) != detection_SFC.end()) {
            /* contained*/
        } else {
            /* not contained */
            false_negatives.push_back(temp);
        }
    }
    
    return false_negatives;
}

inline std::vector<std::pair<int64_t, int64_t>> getFalsePositives(std::vector<std::pair<int64_t, int64_t>> detection_BF, std::vector<std::pair<int64_t, int64_t>> detection_SFC) {

    std::vector<std::pair<int64_t, int64_t>> false_positives;

    //check, whether all elements of BF are also detected by morton (true positives and false positives)
    for(std::pair<int64_t, int64_t> temp : detection_SFC){
        if(std::find(detection_BF.begin(), detection_BF.end(), temp) != detection_BF.end()) {
            /* contained*/
        } else {
            /* not contained */
            false_positives.push_back(temp);
        }
    }
    
    return false_positives;
}


#endif