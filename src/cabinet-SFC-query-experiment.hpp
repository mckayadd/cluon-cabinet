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
#include "cabinet-query-maneuver-brute-force-primitive.hpp"
#include "cabinet-query-maneuver.hpp"

#include <iostream>
#include <sstream>
#include <string>

// Robert Jenkins' 96 bit Mix Function
inline unsigned long mix(unsigned long a, unsigned long b, unsigned long c)
{
    a=a-b;  a=a-c;  a=a^(c >> 13);
    b=b-c;  b=b-a;  b=b^(a << 8);
    c=c-a;  c=c-b;  c=c^(b >> 13);
    a=a-b;  a=a-c;  a=a^(c >> 12);
    b=b-c;  b=b-a;  b=b^(a << 16);
    c=c-a;  c=c-b;  c=c^(b >> 5);
    a=a-b;  a=a-c;  a=a^(c >> 3);
    b=b-c;  b=b-a;  b=b^(a << 10);
    c=c-a;  c=c-b;  c=c^(b >> 15);
    return c;
}

inline float float_rand( float min, float max )
{
    unsigned long seed = mix(clock(), time(NULL), getpid());
    srand(seed);
    float scale = rand() / (float) RAND_MAX; /* [0, 1.0] */
    return round((min + scale * ( max - min )) * 100) / 100;      /* [min, max] */
}

inline uint64_t ts_rand (uint64_t min, uint64_t max) 
{
    unsigned long seed = mix(clock(), time(NULL), getpid());
    srand(seed);
    float scale = rand() / (float) RAND_MAX; /* [0, 1.0] */
    return round((min + scale * ( max - min )));      /* [min, max] */
}

inline std::vector<std::pair<int64_t, int64_t>> getFalseNegatives(std::vector<std::pair<int64_t, int64_t>> detection_BF, std::vector<std::pair<int64_t, int64_t>> detection_SFC) {

    std::vector<std::pair<int64_t, int64_t>> false_negatives;

    if(detection_BF.empty() || detection_SFC.empty()) return false_negatives;

    //check, whether all elements of BF are also detected by morton (true positives and false positives)
    for(std::pair<int64_t, int64_t> temp : detection_BF){
        if(std::find(detection_SFC.begin(), detection_SFC.end(), temp) != detection_SFC.end()) {
            /* contained*/
        } else {
            /* not contained */
            false_negatives.push_back(temp);

            //std::cout << "False Negativ: " << temp.first << ", " << temp.second << std::endl;
        }
    }
    
    return false_negatives;
}

inline std::vector<std::pair<int64_t, int64_t>> getFalsePositives(std::vector<std::pair<int64_t, int64_t>> detection_BF, std::vector<std::pair<int64_t, int64_t>> detection_SFC) {

    std::vector<std::pair<int64_t, int64_t>> false_positives;

    if(detection_BF.empty() || detection_SFC.empty()) return false_positives;

    //check, whether more elements are also detected by morton than BF (false positives)
    for(std::pair<int64_t, int64_t> temp : detection_SFC){
        if(std::find(detection_BF.begin(), detection_BF.end(), temp) != detection_BF.end()) {
            /* contained*/
        } else {
            /* not contained */
            false_positives.push_back(temp);

            //std::cout << "False Positive: " << temp.first << ", " << temp.second << std::endl;
        }
    }
    
    return false_positives;
}


#endif