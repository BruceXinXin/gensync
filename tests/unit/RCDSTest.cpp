//
// Created by bruce on 3/10/2023.
//

#include "RCDSTest.h"

#include "TestAuxiliary.h"
#include <CPISync/Syncs/GenSync.h>
#include <CPISync/Aux_/Auxiliary.h>


CPPUNIT_TEST_SUITE_REGISTRATION(RCDSTest);

void RCDSTest::setUp() {
    const int SEED = 941;
    srand(SEED);

    NTL::ZZ modules;
    modules = 134217689;
    NTL::ZZ_p::init(modules);
}

void RCDSTest::RCDSSetReconcileTest() {
    testAll();
}

void RCDSTest::testAll() {
    GenSync::SyncProtocol protos[1] = {
            GenSync::SyncProtocol::InteractiveCPISync,
//            GenSync::SyncProtocol::IBLTSyncSetDiff
    };
//    GenSync::SyncProtocol protos[1] = {GenSync::SyncProtocol::CPISync};
    shared_ptr<DataObject> fs_server[2] = {
            make_shared<DataObject>("../tests/tmp/dirs_Bob"),
            make_shared<DataObject>("../tests/tmp/dirs_B/Sync.txt")
    };
    shared_ptr<DataObject> fs_client[2] = {
            make_shared<DataObject>("../tests/tmp/dirs_Alice"),
            make_shared<DataObject>("../tests/tmp/dirs_A/Sync.txt")
    };
    shared_ptr<DataObject> fs_client_org[2] = {
            make_shared<DataObject>("../tests/tmp/dirs_Alice_Org"),
            make_shared<DataObject>("../tests/tmp/dirs_A_Org/Sync.txt")
    };
    std::vector<string> reconciled[2] = {
            {},
            {scanTxtFromFile(fs_server[1]->to_string(), numeric_limits<int>::max())}
    };

    auto f_name_list = walkRelDir(fs_server[0]->to_string());
    for (const string &f_name : f_name_list)
        reconciled[0].push_back( scanTxtFromFile(fs_server[0]->to_string() + "/" + f_name, numeric_limits<int>::max()) );

//    fstream fs;
//    fs << system("pwd") << endl;

    for (auto proto: protos)
//        for (int i = 0; i < 2; ++ i) {
        // FIXME: just for debug, change back to i = 1
        for (int i = 1; i >= 0; -- i) {
            GenSync GenSyncServer = GenSync::Builder().
                    setProtocol(GenSync::SyncProtocol::RCDS).
                    setComm(GenSync::SyncComm::socket).
//                    setPort(8003).
                    setRCDSProto(proto).
                    build();

            GenSync GenSyncClient = GenSync::Builder().
                    setProtocol(GenSync::SyncProtocol::RCDS).
                    setComm(GenSync::SyncComm::socket).
//                    setPort(8003).
                    setRCDSProto(proto).
                    build();

            //(oneWay = false, probSync = false, syncParamTest = false, Multiset = false, largeSync = false)
            syncTest(GenSyncClient,
                    GenSyncServer,
                    {fs_server[i]},
                    {fs_client[i]},
                    false);

            // assert
            string filename_client = fs_client[i]->to_string();
            if (isFile(filename_client)) {
                CPPUNIT_ASSERT(
                        scanTxtFromFile(filename_client, numeric_limits<int>::max()) == *reconciled[i].begin()
                        );

                // recover
                char instruction[1000] = {0};
                sprintf(instruction, "cp %s %s", fs_client_org[i]->to_string().c_str(), filename_client.c_str());
                system(instruction);
            }
            else {
                bool res = true;
                auto iter = reconciled[i].begin();
                for (const string &f_name : f_name_list) {
                    res &= (scanTxtFromFile(fs_client[i]->to_string() + "/" + f_name, numeric_limits<int>::max()) == *iter);
                    ++ iter;

                    // recover
                    char instruction[1000] = {0};
                    sprintf(instruction, "cp %s/%s %s/%s", fs_client_org[i]->to_string().c_str(), f_name.c_str(), filename_client.c_str(), f_name.c_str());
                    system(instruction);
                }
                CPPUNIT_ASSERT(res);
            }

        }
}


