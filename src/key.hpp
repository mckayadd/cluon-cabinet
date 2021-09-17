/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "db.hpp"

inline size_t setKey(cabinet::Key k, char *dest, const size_t &len) noexcept {
  const size_t MIN_LEN{sizeof(decltype(k.timeStamp()))
                      +sizeof(decltype(k.dataType()))
                      +sizeof(decltype(k.senderStamp()))
                      +sizeof(decltype(k.hash()))
                      +sizeof(decltype(k.version()))};
  if ( (nullptr != dest) && (MIN_LEN <= len) ) {
    uint16_t offset{0};
    k.accept([](uint32_t, const std::string &, const std::string &) {},
             [dest, &offset](uint32_t field, std::string &&, std::string &&n, auto v) {
              if (5 >= field) {
                decltype(v) hton{v};
                if (2 == sizeof(v)) { hton = htobe16(v); }
                else if (4 == sizeof(v)) { hton = htobe32(v); }
                else if (8 == sizeof(v)) { hton = htobe64(v); }
std::cerr << "writing " << n << ", v = " << v << ", n = " << hton << std::endl;
                std::memcpy(dest + offset, reinterpret_cast<const char*>(&hton), sizeof(hton));
                //std::memcpy(dest + offset, reinterpret_cast<const char*>(&v), sizeof(hton));
								offset += sizeof(hton);
              }
             },
             [](){}
            );
    return offset;   
  }
  return 0;
#if 0 
             // b0-b7: int64_t for timeStamp in nanoseconds
              // b8-b11: int32_t for dataType
              // b12-b15: uint32_t for senderStamp
              // b16-b23: uint64_t for xxhash
              // b24: uint8_t for version

    {
      // version 0:
      // if (511 - (value.mv_size + offset) > 0) --> store value directly in key 
      if ( MAXKEYSIZE > (offset + value.mv_size) )  {
        // b25-b26: uint16_t: length of the value
        const uint16_t length = static_cast<uint16_t>(value.mv_size);
        std::memcpy(_key.data() + offset, reinterpret_cast<const char*>(&length), sizeof(uint16_t));
        offset += sizeof(uint16_t);

        std::memcpy(_key.data() + offset, reinterpret_cast<const char*>(value.mv_data), value.mv_size);
        offset += value.mv_size;
        value.mv_size = 0;
        value.mv_data = 0;
      }
      else {
        // b25-b26: uint16_t: length of the value
        const uint16_t length = 0;
        std::memcpy(_key.data() + offset, reinterpret_cast<const char*>(&length), sizeof(uint16_t));
        offset += sizeof(uint16_t);
      }
    }
  }
#endif
}

inline cabinet::Key getKey(const char *src, const size_t &len) noexcept {
}
