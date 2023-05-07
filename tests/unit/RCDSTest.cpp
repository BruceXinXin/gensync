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

void RCDSTest::BenchMark() {
    GenSync::SyncProtocol proto = GenSync::SyncProtocol::InteractiveCPISync;

    string filename_server = "../tests/rand_file_server.txt";
    string filename_client = "../tests/rand_file_client.txt";
    vector<size_t> sizes; sizes.reserve(100);
    for (size_t i = 1; i <= 60; ++ i)
        sizes.push_back(i * 200000);

    for (size_t size: sizes) {
        // gen rand str
        string rand_str = StringUtils::rand_str(size);
        // save it to server
        writeStrToFile(filename_server, rand_str);

        for (size_t edit = 20; edit <= 20; edit += 10) {
            // edit the copy randomly
            string modified_rand_str = rand_str;
            if (!StringUtils::rand_edit(modified_rand_str, edit))
                CPPUNIT_FAIL("Fail to edit string randomly!");

            // 1. GenSync
            // save
            writeStrToFile(filename_client, modified_rand_str);

            // GenSync Objects
            GenSync GenSyncServer = GenSync::Builder().
                    setProtocol(GenSync::SyncProtocol::RCDS).
                    setComm(GenSync::SyncComm::socket).
                    setRCDSProto(proto).
                    build();

            GenSync GenSyncClient = GenSync::Builder().
                    setProtocol(GenSync::SyncProtocol::RCDS).
                    setComm(GenSync::SyncComm::socket).
                    setRCDSProto(proto).
                    build();

            // Test
            auto start_pq = std::chrono::high_resolution_clock::now();
            syncTest(GenSyncClient,
                     GenSyncServer,
                     {make_shared<DataObject>(filename_server)},
                     {make_shared<DataObject>(filename_client)},
                     false);
            auto stop_pq = std::chrono::high_resolution_clock::now();
//            size_t total_time = std::chrono::duration_cast<std::chrono::microseconds>(stop_pq - start_pq).count();
//            size_t total_bits = GenSyncClient.getXmitBytes(0) + GenSyncClient.getRecvBytes(0);

            // assert
            CPPUNIT_ASSERT(scanWholeTxt(filename_client) == rand_str);

            cout << endl;

//            // TODO record them
//            cout << size << " " << edit << " " << total_time << " " << total_bits << endl;


//            // 2. rsync
//            // save again
//            writeStrToFile(filename_client, modified_rand_str);
//
//            // Test
//            auto res = RSYNC_Test::getRsyncStats(filename_server, filename_client);
//
//            // assert
//            CPPUNIT_ASSERT(scanWholeTxt(filename_client) == rand_str);
//
//            // TODO record them
//            cout << size << " " << edit << " " << res.time << " " << res.recv + res.xmit << endl;
        }
    }


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


// RSYNC_Test
string RSYNC_Test::extractStringIn(string org, string from, string to) {
    auto start = org.find(from);
    if (start == string::npos) return "";
    org = org.substr(start + from.size());
    auto end = org.find(to);
    if (end == string::npos) return "";
    return org.substr(0, end);
}

string RSYNC_Test::subprocess_commandline(const char *command) {
    // borrowed from https://stackoverflow.com/questions/478898/how-do-i-execute-a-command-and-get-output-of-command-within-c-using-posix
    char buffer[128];
    string result = "";
    FILE * pipe = popen(command, "r");
    if (!pipe)
        Logger::error("popen() failed!");
    try {
        while (fgets(buffer, sizeof buffer, pipe) != NULL) {
            result += buffer;
        }
    } catch (exception e) {
        pclose(pipe);
        cout << "We failed to get command line response: " << e.what() << endl;
        Logger::error("Failed to read command reply");
    }
    pclose(pipe);
    return result;
}

// StringUtils
string StringUtils::rand_str(size_t len) {
    string str;
    str.reserve(len);

    for (size_t jj = 0; jj < len; ++jj) {
        auto c = 1 + rand() % 125;  // avoid random string to be "$" changed to "%"
        str += c;

    }
    return str;
}

bool StringUtils::rand_edit(string &str, size_t upper_op) {
    for (size_t i = 0; i < upper_op; ++i) {
        size_t rand_op = rand() % 3;
        switch (rand_op) {
            case 0:
                string_delete(str);
                break;
            case 1:
                string_insert(str);
                break;
            case 2:
                string_modify(str);
                break;
            default:
                return false;
        }
    }

    return true;
}

string StringUtils::delete_char(string str, char c) {
    string res;
    for (char s: str)
        if (s != c)
            res.push_back(s);
    return res;
}

void StringUtils::string_delete(string &str) {
    size_t rand_pos = rand() % str.size();

    str = str.substr(0, rand_pos) + str.substr(rand_pos + 1);
}

void StringUtils::string_insert(string &str) {
    size_t rand_pos = rand() % str.size();

    str = str.substr(0, rand_pos) + string(1, 1 + rand() % 125) + str.substr(rand_pos);
}

void StringUtils::string_modify(string &str) {
    size_t rand_pos = rand() % str.size();

    str[rand_pos] = rand() % 126;
}




