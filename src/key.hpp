/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#ifndef KEY_HPP
#define KEY_HPP

#include "db.hpp"
#include "lmdb.h"

/**
 * This function compares two lmdb keys based on the nanoseconds time stamp.
 *
 * @param a LHS
 * @param b RHS
 * @return -1 iff (LHS < RHS)
 *          0 iff (LHS == RHS)
 *         +1 iff (LHS > RHS)
 */
inline int compareKeys(const MDB_val *a, const MDB_val *b) {
  if (nullptr == a || nullptr == b) {
    return 0;
  }
  if (a->mv_size < sizeof(int64_t) || b->mv_size < sizeof(int64_t)) {
    return 0;
  }
  // b0-b7: int64_t for timeStamp in nanoseconds
  int64_t lhs{*(static_cast<int64_t*>(a->mv_data))};
  int64_t rhs{*(static_cast<int64_t*>(b->mv_data))};
  lhs = be64toh(lhs);
  rhs = be64toh(rhs);
  const int64_t delta{lhs - rhs};
  return (delta < 0 ? -1 : (delta > 0 ? 1 : 0));
};

/**
 * This function writes the data structure cabinet::Key into a char array.
 *
 * @param k Key to write
 * @param dest char array to write to
 * @param len size of the char array
 * @return bytes dumped
 */
inline size_t setKey(cabinet::Key k, char *dest, const size_t &len) noexcept {
  // b0-b7: int64_t for timeStamp in nanoseconds
  // b8-b11: int32_t for dataType
  // b12-b15: uint32_t for senderStamp
  // b16-b23: uint64_t for xxhash
  // b24: uint8_t for version
  const size_t MIN_LEN{sizeof(decltype(k.timeStamp()))
                      +sizeof(decltype(k.dataType()))
                      +sizeof(decltype(k.senderStamp()))
                      +sizeof(decltype(k.hash()))
                      +sizeof(decltype(k.version()))};
  if ( (nullptr != dest) && (MIN_LEN <= len) ) {
    uint16_t offset{0};
    // "visiting" message data structure that describes a key
    k.accept([](uint32_t, const std::string &, const std::string &) {},
             [dest, &offset](uint32_t field, std::string &&, std::string &&, auto v) {
              // only dump the first 5 fields.
              if (5 >= field) {
                // convert values to network byte order.
                decltype(v) hton{v};
                if (2 == sizeof(v)) { hton = htobe16(v); }
                else if (4 == sizeof(v)) { hton = htobe32(v); }
                else if (8 == sizeof(v)) { hton = htobe64(v); }
                // dump values as specified in .odvd file
                std::memcpy(dest + offset, reinterpret_cast<const char*>(&hton), sizeof(hton));
								offset += sizeof(hton);
              }
             },
             [](){}
            );
    return offset;   
  }
  return 0;
}

/**
 * This function extracts the data structure cabinet::Key from a char array.
 *
 * @param src char array to read from
 * @param len size of the char array
 * @return extracted cabinet Key
 */
inline cabinet::Key getKey(const char *src, const size_t &len) noexcept {
  cabinet::Key k;
  const size_t MIN_LEN{sizeof(decltype(k.timeStamp()))
                      +sizeof(decltype(k.dataType()))
                      +sizeof(decltype(k.senderStamp()))
                      +sizeof(decltype(k.hash()))
                      +sizeof(decltype(k.version()))};
  if ( (nullptr != src) && (MIN_LEN <= len) ) {
    uint16_t offset{0};
    // "visiting" message data structure to read a key
    k.accept([](uint32_t, const std::string &, const std::string &) {},
             [src, &offset](uint32_t field, std::string &&, std::string &&, auto &v) {
              // only extract the first 5 fields.
              if (5 >= field) {
                decltype(v) ntoh{v};

                // read values as specified in .odvd file
                std::memcpy(reinterpret_cast<char*>(&ntoh), src + offset, sizeof(ntoh));
								offset += sizeof(ntoh);

                // convert values to host byte order.
                if (2 == sizeof(v)) { ntoh = be16toh(v); }
                else if (4 == sizeof(v)) { ntoh = be32toh(v); }
                else if (8 == sizeof(v)) { ntoh = be64toh(v); }

                v = ntoh;
              }
             },
             [](){}
            );
  }
  return k;
}

#endif
