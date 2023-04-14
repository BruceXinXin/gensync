//
// Created by bruce on 3/15/2023.
//

#include <CPISync/Syncs/GenSync.h>
#include <TestAuxiliary.h>
#include "CPISync/Syncs/RCDS_Synchronizer.h"


inline string randCharacters(int len = 10) {
    string str;

    for (int jj = 0; jj < len; ++jj) {
        int intchar;
        (rand() % 2) ? intchar = randLenBetween(65, 90) : intchar = randLenBetween(97, 122);
        str += toascii(intchar);

    }
    return str;
}

inline forkHandleReport
forkHandle(RCDS_Synchronizer& client,
           RCDS_Synchronizer& server,
           shared_ptr<Communicant>& commCli,
           shared_ptr<Communicant>& commSer,
           vector<shared_ptr<DataObject>>& elems) {
    int err = 1;
    int chld_state;
    int my_opt = 0;
    forkHandleReport result;
    clock_t start = clock();
    try {
        pid_t pID = fork();
        int method_num = 0;
        if (pID == 0) {
//        if (pID > 0) {
            signal(SIGCHLD, SIG_IGN);
            Logger::gLog(Logger::COMM,"created a server process");
//            server.listenSync(method_num,isRecon);
            list<shared_ptr<DataObject>> selfMinusOther, otherMinusSelf;
            bool res = server.SyncServer(commSer, selfMinusOther, otherMinusSelf);
            exit(0);
        } else if (pID < 0) {
            //handle_error("error to fork a child process");
            cout << "throw out err = " << err << endl;
            throw err;
        } else {
            Logger::gLog(Logger::COMM,"created a client process");
//            result.success = client.startSync(method_num,isRecon);
            auto start_time = clock();
            list<shared_ptr<DataObject>> selfMinusOther, otherMinusSelf;
            map<string, double> dummy;
            result.success = client.SyncClient(commCli, selfMinusOther, otherMinusSelf, dummy);
            // 处理Elems, 加入oms, 去除smo(Server端暂时无此需求)
            for (auto& oms: otherMinusSelf)
                elems.emplace_back(move(oms));
            RCDS_Synchronizer::delGroup(elems, selfMinusOther);
            result.totalTime = (double) (clock() - start) / CLOCKS_PER_SEC;
//            result.CPUtime = client.getSyncTime(method_num); /// assuming method_num'th communicator corresponds to method_num'th syncagent
            result.CPUtime = clock() - start_time;
//            result.bytes = client.getXmitBytes(method_num);
            result.bytes = commCli->getXmitBytes();
            waitpid(pID, &chld_state, my_opt);
        }
    } catch (int& err) {
        sleep(1); // why?
        cout << "handle_error caught" << endl;
        result.success=false;
    }

    return result;
}


int main() {
    const int SEED = 941;
    srand(SEED);

    NTL::ZZ modules;
    modules = 134217689;
    NTL::ZZ_p::init(modules);

//    string alicetxt = randAsciiStr(2e4); // 20MB is top on MAC
    string alicetxt = scanTxtFromFile("../tests/tmp/SampleCodeAlice.txt", INT_MAX);
    int partition = 4;
    int lvl = 3;
    int space_c = 8;
    int shingleLen_c = 2;


//    DataObject *atxt = new DataObject(alicetxt);
    auto atxt = make_shared<DataObject>(alicetxt);

    // comm
    auto serverComm = shared_ptr<Communicant>(new CommSocket(8010, "localhost"));
    auto clientComm = shared_ptr<Communicant>(new CommSocket(8010, "localhost"));

    // protocol
    auto Alice = RCDS_Synchronizer(10,
                                   lvl,
                                   partition,
                                   GenSync::SyncProtocol::CPISync,
                                   shingleLen_c,
                                   space_c);
    auto Bob = RCDS_Synchronizer(10,
                                 lvl,
                                 partition,
                                 GenSync::SyncProtocol::CPISync,
                                 shingleLen_c,
                                 space_c);

//    GenSync Alice = GenSync::Builder().
//            setStringProto(GenSync::StringSyncProtocol::SetsOfContent).
//            setProtocol(GenSync::SyncProtocol::IBLTSyncSetDiff).
//            setComm(GenSync::SyncComm::socket).
//            setTerminalStrSize(10).
//            setNumPartitions(partition).
//            setShingleLen(shingleLen_c).
//            setSpace(space_c).
//            setlvl(lvl).
//            setPort(8001).
//            build();


//    string bobtxt = randStringEditBurst(alicetxt, 2e6, "../tests/SampleCodeBob.txt");
    string bobtxt = scanTxtFromFile("../tests/tmp/SampleCodeBob.txt", INT_MAX);
    if (bobtxt.size() < pow(partition, lvl))
        bobtxt += randCharacters(pow(partition, lvl) - bobtxt.size());

//    DataObject *btxt = new DataObject(bobtxt);
    auto btxt = make_shared<DataObject>(bobtxt);

//    GenSync Bob = GenSync::Builder().
//            setStringProto(GenSync::StringSyncProtocol::SetsOfContent).
//            setProtocol(GenSync::SyncProtocol::IBLTSyncSetDiff).
//            setComm(GenSync::SyncComm::socket).
//            setTerminalStrSize(10).
//            setNumPartitions(partition).
//            setShingleLen(shingleLen_c).
//            setSpace(space_c).
//            setlvl(lvl).
//            setPort(8001).
//            build();
    auto str_s = clock();


    vector<shared_ptr<DataObject>> elems;
    Bob.addStr(btxt, elems, false);
    elems.clear();
    double str_time = (double) (clock() - str_s) / CLOCKS_PER_SEC;
    Alice.addStr(atxt, elems, false);


    auto recon_t = clock();
    auto report = forkHandle(Alice, Bob, clientComm, serverComm, elems);
    double recon_time = (double) (clock() - recon_t) / CLOCKS_PER_SEC;
//    string finally = Alice.dumpString()->to_string();
    shared_ptr<DataObject> finally;
    Alice.reconstructString(finally, elems);


//    writeStrToFile("Alice.txt", alicetxt);
//    writeStrToFile("Bob.txt", bobtxt);

//    cout << "Set of Content cost: " << to_string(report.bytesRTot + report.bytesXTot) << endl;
    cout << "Set of Content cost: " << report.bytes << endl;

    cout << "CPU Time: " + to_string(report.CPUtime) << endl;
    cout << "Time: " + to_string(report.totalTime) << endl;
//    cout << "bitsTot: " + to_string(report.bytesXTot) << endl;
//    cout << "bitsR: " + to_string(report.bytesRTot) << endl;

    cout << "String Add Time: " << str_time << endl;
    cout << "Rest of the Recon time: " << recon_time << endl;
//    delete btxt;
//    delete atxt;
    cout << (finally->to_string() == bobtxt) << endl;
    cout << report.success << endl;
    cout << "SetsOfContent Success!" << endl;

    return 0;
}