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

TEST_CASE("Range querying Morton-indexed GPS traces") {
  const char *gps = R"(
57.77778901 12.77790337
57.77734125 12.77817215
57.77689127 12.77846196
57.77645616 12.77873826
57.77601406 12.77902484
57.77556784 12.77930998
57.7751248 12.77957598
57.77467775 12.77975093
57.77422499 12.77980395
57.7737736 12.77971304
57.77332939 12.77949689
57.77289907 12.77913649
57.77250135 12.77864161
57.77216032 12.77803764
57.77187986 12.77735772
57.77166482 12.77660913
57.77151726 12.77580491
57.77143545 12.77494498
57.77141137 12.7740273
57.77140439 12.77307387
57.77140376 12.77214181
57.77143701 12.77125741
57.77152257 12.77038774
57.77165858 12.76955397
57.77184604 12.76874014
57.77209332 12.76794528
57.77238912 12.76722763
57.77271621 12.76659785
57.77306859 12.76606175
57.77346526 12.76559187
57.77388869 12.7652073
57.77434951 12.76490549
57.77481957 12.76472222
57.77528843 12.76462972
57.77575132 12.76465133
57.77621227 12.76477113
57.7766734 12.76499178
57.77711784 12.76531837
57.77754423 12.7657113
57.77796038 12.76611514
57.77839225 12.76651221
57.77884971 12.76681376
57.77931949 12.76699075
57.77979492 12.76706398
57.78026842 12.76701259
57.78074233 12.76683419
57.78119314 12.76654933
57.78161167 12.76617196
57.78200969 12.76573402
57.78240413 12.76527802
57.78281179 12.76480606
57.78322918 12.76433381
57.78365049 12.76384777
57.78403756 12.76339636
57.7844396 12.76293542
57.78485236 12.76245808
57.78526512 12.76198531
57.78567563 12.76151412
57.78607437 12.76105485
57.78646903 12.76059934
57.78685972 12.76015645
57.78729273 12.75972409
57.78772906 12.7594104
57.78820149 12.75920448
57.78868953 12.75913302
57.78917059 12.75919195
57.78962594 12.75937305
57.79006776 12.75967779
57.79048793 12.76009748
57.79085991 12.76061729
57.7912018 12.76126108
57.79147693 12.76200414
57.79167706 12.76284685
57.79178783 12.76368688
57.79182191 12.7645425
57.79177819 12.76539516
57.79164308 12.76628887
57.79142382 12.76711409
57.79114957 12.76781077
57.79081165 12.76841176
57.79043384 12.76887687
57.79000543 12.76924681
57.78957781 12.7695419
57.78914941 12.7698457
57.78870819 12.77020802
57.78828911 12.77061578
57.78788781 12.77107466
57.78749761 12.77156274
57.78711964 12.77203214
57.7867304 12.77251473
57.78631662 12.7730335
57.7859302 12.77351976
57.78554044 12.77400062
57.78514573 12.77446785
57.7847399 12.77489121
57.78432775 12.77527039
57.78391069 12.77560755
57.78348518 12.77590203
57.78300759 12.7761827
57.78254842 12.77639931
57.78209549 12.77657095
57.78162585 12.7766985
57.78116643 12.77678231
57.7807129 12.77686025
57.78026355 12.77694265
57.77977698 12.77705753
57.7793023 12.77720993
57.77884228 12.7773841
57.77837325 12.77759546
)";

  std::string RECFILE("az.rec");
  std::string DBFILE("az.cab");
  {
    UNLINK(RECFILE.c_str());
    UNLINK("az.cab");
    UNLINK("az.cab-lock");
    std::fstream recordingFile(RECFILE.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
    REQUIRE(recordingFile.good());
   
    const std::string GPS(gps);
    std::stringstream sstr(GPS);
    std::string line;
    int64_t timeStamp{0};
    while (std::getline(sstr, line)) {
      std::stringstream s(line);
      float lat, lon;
      s >> lat >> lon;
      //std::cout << "lat = " << lat << ", " << lon << std::endl;
   
      opendlv::proxy::GeodeticWgs84Reading w;
      w.latitude(lat);
      w.longitude(lon);

      cluon::ToProtoVisitor proto;
      w.accept(proto);

      cluon::data::Envelope env;
      cluon::data::TimeStamp sent;
      sent.seconds(101+timeStamp).microseconds(1001+timeStamp);
      cluon::data::TimeStamp received;
      received.seconds(102+timeStamp).microseconds(1002+timeStamp);
      cluon::data::TimeStamp sampleTimeStamp;
      sampleTimeStamp.seconds(103+timeStamp).microseconds(1003+timeStamp);

      env.serializedData(proto.encodedData());
      env.dataType(opendlv::proxy::GeodeticWgs84Reading::ID()).sent(sent).received(received).sampleTimeStamp(sampleTimeStamp);

      const std::string tmp{cluon::serializeEnvelope(std::move(env))};
      recordingFile.write(tmp.c_str(), static_cast<std::streamsize>(tmp.size()));
      recordingFile.flush();

      timeStamp += 10;
    }
  }

  const bool VERBOSE{true};
  const uint64_t MEM{1};
  const int64_t SIZE_DB = MEM * 1024UL * 1024UL * 1024UL;
  REQUIRE(0 == rec2cabinet("tests-morton", MEM, RECFILE, DBFILE, VERBOSE));

  MDB_env *env{nullptr};
  const int numberOfDatabases{100};
  REQUIRE(MDB_SUCCESS == mdb_env_create(&env));
  REQUIRE(MDB_SUCCESS == mdb_env_set_maxdbs(env, numberOfDatabases));
  REQUIRE(MDB_SUCCESS == mdb_env_set_mapsize(env, SIZE_DB));
  REQUIRE(MDB_SUCCESS == mdb_env_open(env, DBFILE.c_str(), MDB_NOSUBDIR|MDB_RDONLY, 0600));
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
    REQUIRE(110 == stat.ms_entries);

    MDB_cursor *cursor;
    REQUIRE(MDB_SUCCESS == mdb_cursor_open(txn, dbi, &cursor));
    {
      MDB_val key;
      MDB_val value;

      while (mdb_cursor_get(cursor, &key, &value, MDB_NEXT) == 0) {
        uint64_t morton = *reinterpret_cast<uint64_t*>(key.mv_data);
        morton = be64toh(morton);
        auto decodedLatLon = convertMortonToLatLon(morton);
        int64_t timeStamp{0};
        if (value.mv_size == sizeof(int64_t)) {
          const char *ptr = static_cast<char*>(value.mv_data);
          std::memcpy(&timeStamp, ptr, value.mv_size);
          timeStamp = be64toh(timeStamp);
          std::cout << morton << "(" << decodedLatLon.first << "," << decodedLatLon.second << "): " << timeStamp << std::endl;
        }
      }

#if 0
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
#endif
    }
 
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
  }
   
  if (env) {
    mdb_env_close(env);
  }
 


  UNLINK(RECFILE.c_str());
  UNLINK("az.cab");
  UNLINK("az.cab-lock");
}
