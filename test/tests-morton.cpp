/*
 * Copyright (C) 2021  Christian Berger
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "catch.hpp"

#ifdef WIN32
    #define UNLINK _unlink
#else
    #include <unistd.h>
    #define UNLINK unlink
#endif

#include "cluon-complete.hpp"
#include "opendlv-standard-message-set.hpp"
#include "rec2cabinet.hpp"
#include "morton.hpp"
#include "lmdb.h"

#include <fstream>
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

TEST_CASE("Test GPS coordinates TR") {
  const std::pair<float,float> a(60.734398f,14.768745f);
  auto a_mc = convertLatLonToMorton(a);
  REQUIRE(a_mc == 664749436224649);
  auto d_a_mc = convertMortonToLatLon(a_mc);
  REQUIRE(d_a_mc.first == Approx(a.first));
  REQUIRE(d_a_mc.second == Approx(a.second));
//std::cout << a.first << ", " << a.second << " " << a_mc << "," << d_a_mc.first << ", " << d_a_mc.second << std::endl;
}

TEST_CASE("Test GPS coordinates TL") {
  const std::pair<float,float> a(38.969745f,-77.201958f);
  auto a_mc = convertLatLonToMorton(a);
  REQUIRE(a_mc == 231657429695220);
  auto d_a_mc = convertMortonToLatLon(a_mc);
  REQUIRE(d_a_mc.first == Approx(a.first));
  REQUIRE(d_a_mc.second == Approx(a.second));
//std::cout << a.first << ", " << a.second << " " << a_mc << "," << d_a_mc.first << ", " << d_a_mc.second << std::endl;
}

TEST_CASE("Test GPS coordinates BL") {
  const std::pair<float,float> a(-34.619055f,-58.364067f);
  auto a_mc = convertLatLonToMorton(a);
  REQUIRE(a_mc == 171054631290071);
  auto d_a_mc = convertMortonToLatLon(a_mc);
  REQUIRE(d_a_mc.first == Approx(a.first));
  REQUIRE(d_a_mc.second == Approx(a.second));
//std::cout << a.first << ", " << a.second << " " << a_mc << "," << d_a_mc.first << ", " << d_a_mc.second << std::endl;
}

TEST_CASE("Test GPS coordinates BR") {
  const std::pair<float,float> a(-33.956603f,150.949719f);
  auto a_mc = convertLatLonToMorton(a);
  REQUIRE(a_mc == 769185334910872);
  auto d_a_mc = convertMortonToLatLon(a_mc);
  REQUIRE(d_a_mc.first == Approx(a.first));
  REQUIRE(d_a_mc.second == Approx(a.second));
//std::cout << a.first << ", " << a.second << " " << a_mc << "," << d_a_mc.first << ", " << d_a_mc.second << std::endl;
}

TEST_CASE("Test GeodeticWge84") {
  std::string RECFILE("wgs.rec");
  std::string DBFILE("wgs.cab");
  UNLINK(RECFILE.c_str());
  UNLINK("wgs.cab");
  UNLINK("wgs.cab-lock");
  std::fstream recordingFile(RECFILE.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
  REQUIRE(recordingFile.good());
  
  // Three shall end up in the same Morton bucket but ordered temporally correct.
  // Preparing data 1/5
  {
    opendlv::proxy::GeodeticWgs84Reading w;
    w.latitude(1.234567f);
    w.longitude(9.876543f);

    cluon::ToProtoVisitor proto;
    w.accept(proto);

    cluon::data::Envelope env;
    cluon::data::TimeStamp sent;
    sent.seconds(101).microseconds(1001);
    cluon::data::TimeStamp received;
    received.seconds(102).microseconds(1002);
    cluon::data::TimeStamp sampleTimeStamp;
    sampleTimeStamp.seconds(103).microseconds(1003);

    env.serializedData(proto.encodedData());
    env.dataType(opendlv::proxy::GeodeticWgs84Reading::ID()).sent(sent).received(received).sampleTimeStamp(sampleTimeStamp);

    const std::string tmp{cluon::serializeEnvelope(std::move(env))};
    recordingFile.write(tmp.c_str(), static_cast<std::streamsize>(tmp.size()));
    recordingFile.flush();
  }
  // Preparing data 2/5
  {
    opendlv::proxy::GeodeticWgs84Reading w;
    w.latitude(1.234565f);
    w.longitude(9.876543f);

    cluon::ToProtoVisitor proto;
    w.accept(proto);

    cluon::data::Envelope env;
    cluon::data::TimeStamp sent;
    sent.seconds(101).microseconds(1000);
    cluon::data::TimeStamp received;
    received.seconds(102).microseconds(1001);
    cluon::data::TimeStamp sampleTimeStamp;
    sampleTimeStamp.seconds(103).microseconds(1002);

    env.serializedData(proto.encodedData());
    env.dataType(opendlv::proxy::GeodeticWgs84Reading::ID()).sent(sent).received(received).sampleTimeStamp(sampleTimeStamp);

    const std::string tmp{cluon::serializeEnvelope(std::move(env))};
    recordingFile.write(tmp.c_str(), static_cast<std::streamsize>(tmp.size()));
    recordingFile.flush();
  }
  // Preparing data 3/5
  {
    opendlv::proxy::GeodeticWgs84Reading w;
    w.latitude(1.234568f);
    w.longitude(9.876544f);

    cluon::ToProtoVisitor proto;
    w.accept(proto);

    cluon::data::Envelope env;
    cluon::data::TimeStamp sent;
    sent.seconds(101).microseconds(1002);
    cluon::data::TimeStamp received;
    received.seconds(102).microseconds(1003);
    cluon::data::TimeStamp sampleTimeStamp;
    sampleTimeStamp.seconds(103).microseconds(1004);

    env.serializedData(proto.encodedData());
    env.dataType(opendlv::proxy::GeodeticWgs84Reading::ID()).sent(sent).received(received).sampleTimeStamp(sampleTimeStamp);

    const std::string tmp{cluon::serializeEnvelope(std::move(env))};
    recordingFile.write(tmp.c_str(), static_cast<std::streamsize>(tmp.size()));
    recordingFile.flush();
  }
  // Preparing data 4/5
  {
    opendlv::proxy::GeodeticWgs84Reading w;
    w.latitude(1.334567f);
    w.longitude(9.076543f);

    cluon::ToProtoVisitor proto;
    w.accept(proto);

    cluon::data::Envelope env;
    cluon::data::TimeStamp sent;
    sent.seconds(101).microseconds(1000);
    cluon::data::TimeStamp received;
    received.seconds(102).microseconds(1000);
    cluon::data::TimeStamp sampleTimeStamp;
    sampleTimeStamp.seconds(103).microseconds(1000);

    env.serializedData(proto.encodedData());
    env.dataType(opendlv::proxy::GeodeticWgs84Reading::ID()).sent(sent).received(received).sampleTimeStamp(sampleTimeStamp);

    const std::string tmp{cluon::serializeEnvelope(std::move(env))};
    recordingFile.write(tmp.c_str(), static_cast<std::streamsize>(tmp.size()));
    recordingFile.flush();
  }
  // Preparing data 5/5
  {
    opendlv::proxy::GeodeticWgs84Reading w;
    w.latitude(10.234567f);
    w.longitude(21.876543f);

    cluon::ToProtoVisitor proto;
    w.accept(proto);

    cluon::data::Envelope env;
    cluon::data::TimeStamp sent;
    sent.seconds(101).microseconds(1005);
    cluon::data::TimeStamp received;
    received.seconds(102).microseconds(1006);
    cluon::data::TimeStamp sampleTimeStamp;
    sampleTimeStamp.seconds(99).microseconds(1005);

    env.serializedData(proto.encodedData());
    env.dataType(opendlv::proxy::GeodeticWgs84Reading::ID()).sent(sent).received(received).sampleTimeStamp(sampleTimeStamp);

    const std::string tmp{cluon::serializeEnvelope(std::move(env))};
    recordingFile.write(tmp.c_str(), static_cast<std::streamsize>(tmp.size()));
    recordingFile.flush();
  }
  recordingFile.close();

  const bool VERBOSE{true};
  const uint64_t MEM{1};
  const int64_t SIZE_DB = MEM * 1024UL * 1024UL * 1024UL;
  REQUIRE(0 == rec2cabinet("tests-morton", MEM, RECFILE, DBFILE, VERBOSE));

//1.23457, 9.87654 = 642422142713897, 103001003000
//1.23457, 9.87654 = 642422142713897, 103001002000
//1.23457, 9.87654 = 642422142713897, 103001004000
//1.33457, 9.07654 = 642413581910312, 103001000000
//10.2346, 11.87654 = 645827077604393, 99001005000

  MDB_env *env{nullptr};
  const int numberOfDatabases{100};
  REQUIRE(MDB_SUCCESS == mdb_env_create(&env));
  REQUIRE(MDB_SUCCESS == mdb_env_set_maxdbs(env, numberOfDatabases));
  REQUIRE(MDB_SUCCESS == mdb_env_set_mapsize(env, SIZE_DB));
  REQUIRE(MDB_SUCCESS == mdb_env_open(env, DBFILE.c_str(), MDB_NOSUBDIR|MDB_RDONLY, 0600));

  // Read back the data from the DB; first temporal order in 'all' must be: 5,4,2,1,3
  {
    MDB_txn *txn{nullptr};
    MDB_dbi dbi{0};
    REQUIRE(MDB_SUCCESS == mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn));
    REQUIRE(MDB_SUCCESS == mdb_dbi_open(txn, "all", 0 , &dbi));
    mdb_set_compare(txn, dbi, &compareKeys);

    MDB_stat stat;
    mdb_stat(txn, dbi, &stat);
    REQUIRE(5 == stat.ms_entries);

    MDB_cursor *cursor;
    REQUIRE(MDB_SUCCESS == mdb_cursor_open(txn, dbi, &cursor));
    {
      MDB_val key;
      MDB_val value;
      REQUIRE(0 == mdb_cursor_get(cursor, &key, &value, MDB_NEXT));
      {
        const char *ptr = static_cast<char*>(key.mv_data);
        cabinet::Key storedKey = getKey(ptr, key.mv_size);
        REQUIRE(storedKey.timeStamp() == 99001005000);
        //std::cout << storedKey.timeStamp() << ": " << storedKey.dataType() << "/" << storedKey.senderStamp() << std::endl;
      }
    }
    {
      MDB_val key;
      MDB_val value;
      REQUIRE(0 == mdb_cursor_get(cursor, &key, &value, MDB_NEXT));
      {
        const char *ptr = static_cast<char*>(key.mv_data);
        cabinet::Key storedKey = getKey(ptr, key.mv_size);
        REQUIRE(storedKey.timeStamp() == 103001000000);
        //std::cout << storedKey.timeStamp() << ": " << storedKey.dataType() << "/" << storedKey.senderStamp() << std::endl;
      }
    }
    {
      MDB_val key;
      MDB_val value;
      REQUIRE(0 == mdb_cursor_get(cursor, &key, &value, MDB_NEXT));
      {
        const char *ptr = static_cast<char*>(key.mv_data);
        cabinet::Key storedKey = getKey(ptr, key.mv_size);
        REQUIRE(storedKey.timeStamp() == 103001002000);
        //std::cout << storedKey.timeStamp() << ": " << storedKey.dataType() << "/" << storedKey.senderStamp() << std::endl;
      }
    }
    {
      MDB_val key;
      MDB_val value;
      REQUIRE(0 == mdb_cursor_get(cursor, &key, &value, MDB_NEXT));
      {
        const char *ptr = static_cast<char*>(key.mv_data);
        cabinet::Key storedKey = getKey(ptr, key.mv_size);
        REQUIRE(storedKey.timeStamp() == 103001003000);
        //std::cout << storedKey.timeStamp() << ": " << storedKey.dataType() << "/" << storedKey.senderStamp() << std::endl;
      }
    }
    {
      MDB_val key;
      MDB_val value;
      REQUIRE(0 == mdb_cursor_get(cursor, &key, &value, MDB_NEXT));
      {
        const char *ptr = static_cast<char*>(key.mv_data);
        cabinet::Key storedKey = getKey(ptr, key.mv_size);
        REQUIRE(storedKey.timeStamp() == 103001004000);
        //std::cout << storedKey.timeStamp() << ": " << storedKey.dataType() << "/" << storedKey.senderStamp() << std::endl;
      }

      REQUIRE(0 != mdb_cursor_get(cursor, &key, &value, MDB_NEXT));
    }
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
  }

  // Read back the data from the DB; first temporal order in '19/0-morton' must be: 4,2,1,3,5
  {
    MDB_txn *txn{nullptr};
    MDB_dbi dbi{0};
    REQUIRE(MDB_SUCCESS == mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn));
    REQUIRE(MDB_SUCCESS == mdb_dbi_open(txn, "19/0-morton", 0 , &dbi));
    mdb_set_compare(txn, dbi, &compareMortonKeys);
    // Multiple values are stored by existing timeStamp in nanoseconds.
    mdb_set_dupsort(txn, dbi, &compareKeys);
 
    MDB_stat stat;
    mdb_stat(txn, dbi, &stat);
    REQUIRE(5 == stat.ms_entries);

    MDB_cursor *cursor;
    REQUIRE(MDB_SUCCESS == mdb_cursor_open(txn, dbi, &cursor));
    {
      MDB_val key;
      MDB_val value;
      REQUIRE(0 == mdb_cursor_get(cursor, &key, &value, MDB_NEXT));
      {
        uint64_t morton = *reinterpret_cast<uint64_t*>(key.mv_data);
        morton = be64toh(morton);
        int64_t timeStamp = *reinterpret_cast<int64_t*>(value.mv_data);
        timeStamp = be64toh(timeStamp);
        std::cout << morton << " " << timeStamp << std::endl;
        REQUIRE(morton == 642413581910312);
        REQUIRE(timeStamp == 103001000000);
      }
    }
    {
      MDB_val key;
      MDB_val value;
      REQUIRE(0 == mdb_cursor_get(cursor, &key, &value, MDB_NEXT));
      {
        uint64_t morton = *reinterpret_cast<uint64_t*>(key.mv_data);
        morton = be64toh(morton);
        int64_t timeStamp = *reinterpret_cast<int64_t*>(value.mv_data);
        timeStamp = be64toh(timeStamp);
        std::cout << morton << " " << timeStamp << std::endl;
        REQUIRE(morton == 642422142713897);
        REQUIRE(timeStamp == 103001002000);
      }
    }
    {
      MDB_val key;
      MDB_val value;
      REQUIRE(0 == mdb_cursor_get(cursor, &key, &value, MDB_NEXT));
      {
        uint64_t morton = *reinterpret_cast<uint64_t*>(key.mv_data);
        morton = be64toh(morton);
        int64_t timeStamp = *reinterpret_cast<int64_t*>(value.mv_data);
        timeStamp = be64toh(timeStamp);
        std::cout << morton << " " << timeStamp << std::endl;
        REQUIRE(morton == 642422142713897);
        REQUIRE(timeStamp == 103001003000);
      }
    }
    {
      MDB_val key;
      MDB_val value;
      REQUIRE(0 == mdb_cursor_get(cursor, &key, &value, MDB_NEXT));
      {
        uint64_t morton = *reinterpret_cast<uint64_t*>(key.mv_data);
        morton = be64toh(morton);
        int64_t timeStamp = *reinterpret_cast<int64_t*>(value.mv_data);
        timeStamp = be64toh(timeStamp);
        std::cout << morton << " " << timeStamp << std::endl;
        REQUIRE(morton == 642422142713897);
        REQUIRE(timeStamp == 103001004000);
      }
    }
    {
      MDB_val key;
      MDB_val value;
      REQUIRE(0 == mdb_cursor_get(cursor, &key, &value, MDB_NEXT));
      {
        uint64_t morton = *reinterpret_cast<uint64_t*>(key.mv_data);
        morton = be64toh(morton);
        int64_t timeStamp = *reinterpret_cast<int64_t*>(value.mv_data);
        timeStamp = be64toh(timeStamp);
        std::cout << morton << " " << timeStamp << std::endl;
        REQUIRE(morton == 645827077604393);
        REQUIRE(timeStamp == 99001005000);
      }
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
  }
   
  if (env) {
    mdb_env_close(env);
  }
 
  UNLINK("wgs.rec");
  UNLINK("wgs.cab");
  UNLINK("wgs.cab-lock");
}

