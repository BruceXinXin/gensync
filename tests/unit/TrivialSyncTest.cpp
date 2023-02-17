//
// Created by bruce on 2/7/2023.
//

#include "TrivialSyncTest.h"
#include "TestAuxiliary.h"

CPPUNIT_TEST_SUITE_REGISTRATION(TrivialSyncTest);

TrivialSyncTest::TrivialSyncTest() = default;

TrivialSyncTest::~TrivialSyncTest() = default;

void TrivialSyncTest::setUp() {
    const int SEED = 941;
    srand(SEED);
}

void TrivialSyncTest::tearDown() { }

void TrivialSyncTest::TrivialSyncSetReconcileTest() {
    GenSync GenSyncServer = GenSync::Builder().
            setProtocol(GenSync::SyncProtocol::TrivialSync).
            setComm(GenSync::SyncComm::socket).
            build();

    GenSync GenSyncClient = GenSync::Builder().
            setProtocol(GenSync::SyncProtocol::TrivialSync).
            setComm(GenSync::SyncComm::socket).
            build();
    //(oneWay = false, probSync = false, syncParamTest = false, Multiset = false, largeSync = false)
    CPPUNIT_ASSERT(syncTest(GenSyncClient, GenSyncServer, false, false, false, false, false));
}

void TrivialSyncTest::TrivialSyncMultisetReconcileTest(){
    GenSync GenSyncServer = GenSync::Builder().
            setProtocol(GenSync::SyncProtocol::TrivialSync).
            setComm(GenSync::SyncComm::socket).
            build();

    GenSync GenSyncClient = GenSync::Builder().
            setProtocol(GenSync::SyncProtocol::TrivialSync).
            setComm(GenSync::SyncComm::socket).
            build();

    //(oneWay = false, probSync = false, syncParamTest = false, Multiset = true, largeSync = false)
    CPPUNIT_ASSERT(syncTest(GenSyncClient, GenSyncServer, false, false, false, true, false));
}

void TrivialSyncTest::TrivialSyncLargeSetReconcileTest() {
    GenSync GenSyncServer = GenSync::Builder().
            setProtocol(GenSync::SyncProtocol::TrivialSync).
            setComm(GenSync::SyncComm::socket).
            build();

    GenSync GenSyncClient = GenSync::Builder().
            setProtocol(GenSync::SyncProtocol::TrivialSync).
            setComm(GenSync::SyncComm::socket).
            build();

    //(oneWay = false, probSync = false, syncParamTest = false, Multiset = false, largeSync = true)
    CPPUNIT_ASSERT(syncTest(GenSyncClient, GenSyncServer, false, false, false, false, true));
}