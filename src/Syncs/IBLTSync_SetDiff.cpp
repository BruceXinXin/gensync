//
// Created by Bowen Song on 10/14/18.
//

#include <CPISync/Syncs/IBLTSync_SetDiff.h>

IBLTSync_SetDiff::IBLTSync_SetDiff(size_t expected_diff, size_t eltSize, bool keep_alive) : myIBLT((expected_diff>0)?expected_diff:1, eltSize) {
    expNumDiff = expected_diff;
    oneWay = false;
    keepAlive = keep_alive;
}
IBLTSync_SetDiff::~IBLTSync_SetDiff(){};

bool IBLTSync_SetDiff::SyncClient(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                                  list<shared_ptr<DataObject>> &otherMinusSelf) {
    Logger::gLog(Logger::METHOD, "Entering IBLTSync::SyncClient");

    bool success = true;
if(!keepAlive) {
    // call parent method for bookkeeping
    SyncMethod::SyncClient(commSync, selfMinusOther, otherMinusSelf);
    // connect to server
    commSync->commConnect();
}
    // ensure that the IBLT size and eltSize equal those of the server
    if(!commSync->establishIBLTSend(myIBLT.size(), myIBLT.eltSize(), oneWay)) {
        Logger::gLog(Logger::METHOD_DETAILS,
                     "IBLT parameters do not match up between client and server!");
        success = false;
    }
    commSync->commSend(myIBLT, true);

    if(!oneWay) {
        list<shared_ptr<DataObject>> newOMS = commSync->commRecv_DataObject_List();
        list<shared_ptr<DataObject>> newSMO = commSync->commRecv_DataObject_List();

        otherMinusSelf.insert(otherMinusSelf.end(), newOMS.begin(), newOMS.end());
        selfMinusOther.insert(selfMinusOther.end(), newSMO.begin(), newSMO.end());

        stringstream msg;
        msg << "IBLTSync_SetDiff succeeded." << endl;
        msg << "self - other = " << printListOfSharedPtrs(selfMinusOther) << endl;
        msg << "other - self = " << printListOfSharedPtrs(otherMinusSelf) << endl;
        Logger::gLog(Logger::METHOD, msg.str());
    }
//    success = ((SYNC_SUCCESS==commSync->commRecv_int()) and success);
//    (success)? commSync->commSend(SYNC_SUCCESS) : commSync->commSend(SYNC_FAILURE);
    return success;
}

bool IBLTSync_SetDiff::SyncServer(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                                  list<shared_ptr<DataObject>> &otherMinusSelf) {
    Logger::gLog(Logger::METHOD, "Entering IBLTSync::SyncServer");

    bool success = true;

    if(!keepAlive) {
        // call parent method for bookkeeping
        SyncMethod::SyncServer(commSync, selfMinusOther, otherMinusSelf);

        // listen for client
        commSync->commListen();
    }
    // ensure that the IBLT size and eltSize equal those of the server
    if(!commSync->establishIBLTRecv(myIBLT.size(), myIBLT.eltSize(), oneWay)) {
        Logger::gLog(Logger::METHOD_DETAILS,
                     "IBLT parameters do not match up between client and server!");
        success = false;
    }

    // verified that our size and eltSize == theirs
    IBLT theirs = commSync->commRecv_IBLT(myIBLT.size(), myIBLT.eltSize());

    // more efficient than - and modifies theirs, which we don't care about
    vector<pair<ZZ, ZZ>> positive, negative;

    if(!(theirs-=myIBLT).listEntries(positive, negative)) {
        Logger::gLog(Logger::METHOD_DETAILS,
                     "Unable to completely reconcile, returning a partial list of differences");
        success = false;
    }

    // store values because they're what we care about
    for(auto pair : positive) {
        otherMinusSelf.emplace_back(new DataObject(pair.second));
    }

    for(auto pair : negative) {
        selfMinusOther.emplace_back(new DataObject(pair.first));
    }

    // send the difference
    if(!oneWay) {
        commSync->commSend(selfMinusOther);
        commSync->commSend(otherMinusSelf);
    }

    stringstream msg;
    msg << "IBLTSync_SetDiff " << (success ? "succeeded" : "may not have completely succeeded") << endl;
    // 修改, 使用已有的函数
    msg << "self - other = " << printListOfSharedPtrs(selfMinusOther) << endl;
    msg << "other - self = " << printListOfSharedPtrs(otherMinusSelf) << endl;
    Logger::gLog(Logger::METHOD, msg.str());


//    (success)? commSync->commSend(SYNC_SUCCESS) : commSync->commSend(SYNC_FAILURE);
//    return (SYNC_SUCCESS==commSync->commRecv_int()) and success;
    return success;
}

bool IBLTSync_SetDiff::addElem(shared_ptr<DataObject> datum) {
    SyncMethod::addElem(datum);
    myIBLT.insert(datum->to_ZZ(), datum->to_ZZ());
    return true;
}

bool IBLTSync_SetDiff::delElem(shared_ptr<DataObject> datum) {
    SyncMethod::delElem(datum);
    myIBLT.erase(datum->to_ZZ(), datum->to_ZZ());
    return true;
}

string IBLTSync_SetDiff::getName() {return "I am an IBLTSync with the following params:\n*expected number of set diff: "
+ toStr(expNumDiff) + "\n*size of values: " + toStr(myIBLT.eltSize());}