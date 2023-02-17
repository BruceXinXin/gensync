//
// Created by bruce on 2/5/2023.
//

#include <CPISync/Syncs/GenSync.h>
#include <TestAuxiliary.h>
#include "CPISync/Syncs/TrivialSync.h"


int main() {
    const int SEED = 941;
    srand(SEED);

    NTL::ZZ modules;
    modules = 134217689;
    NTL::ZZ_p::init(modules);


    TrivialSync tSync;

    GenSync GenSyncServer = GenSync::Builder().
            setProtocol(GenSync::SyncProtocol::TrivialSync).
            setComm(GenSync::SyncComm::socket).
            build();

    GenSync GenSyncClient = GenSync::Builder().
            setProtocol(GenSync::SyncProtocol::TrivialSync).
            setComm(GenSync::SyncComm::socket).
            build();

    //(oneWay = false, probSync = false, syncParamTest = false, Multiset = false, largeSync = true)
    cout << syncTest(GenSyncClient, GenSyncServer, false, false, false, false, true) << endl;


    return 0;
}