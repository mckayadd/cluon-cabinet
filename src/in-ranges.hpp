/*
 * Copyright (C) 2022  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Inspired by https://www.tutorialspoint.com/queries-to-check-if-a-number-lies-in-n-ranges-of-l-r-in-cplusplus
 */

#ifndef IN_RANGES
#define IN_RANGES

#include <algorithm>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace cluon {

template <class T>
class In_Ranges {
 private:
  In_Ranges(const In_Ranges &) = delete;
  In_Ranges(In_Ranges &&)      = delete;
  In_Ranges &operator=(const In_Ranges &) = delete;
  In_Ranges &operator=(In_Ranges &&) = delete;

 public:
  In_Ranges() = default;

  /**
   * @param range Range to be added
   */
  void addRange(std::pair<T, T> range) {
    std::lock_guard<std::mutex> lck(m_mutex);
    m_rangesList.push_back(range.first);
    m_rangesMap[range.first] = 1;
    m_rangesList.push_back(range.second);
    m_rangesMap[range.second] = 2;

    m_sorted = false;
  }

  bool isInAnyRange(const T &val) {
    std::lock_guard<std::mutex> lck(m_mutex);
    if (!m_sorted) {
      std::sort(m_rangesList.begin(), m_rangesList.end());
      m_sorted = true;
    }
    
    const auto idx = std::lower_bound(m_rangesList.begin(), m_rangesList.end(), val) - m_rangesList.begin();
    return !m_rangesList.empty() && ( (val == m_rangesList[idx]) ? true : ( (2 == m_rangesMap[m_rangesList[idx]]) ? true : false) );
  }

 private:
  std::mutex m_mutex = {};
  bool m_sorted{false};
  std::vector<T> m_rangesList = {};
  std::unordered_map<T, T> m_rangesMap = {};
};

} // cluon
#endif
