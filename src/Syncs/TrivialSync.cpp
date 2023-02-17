//
// Created by bruce on 2/5/2023.
//

#include <CPISync/Aux_/Exceptions.h>
#include "CPISync/Syncs/TrivialSync.h"


TrivialSync::TrivialSync() = default;

TrivialSync::~TrivialSync() = default;


bool TrivialSync::SyncClient(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                             list<shared_ptr<DataObject>> &otherMinusSelf) {
    try {
        Logger::gLog(Logger::METHOD, "TSync::SyncClient");

        SyncMethod::SyncClient(commSync, selfMinusOther, otherMinusSelf);

        // connect
        mySyncStats.timerStart(SyncStats::IDLE_TIME);
        commSync->commConnect();
        mySyncStats.timerEnd(SyncStats::IDLE_TIME);

        // send
        mySyncStats.timerStart(SyncStats::COMM_TIME);
        commSync->commSendSequenceOfSDO(SyncMethod::getElements());

        // receive
        commSync->commRecvSequenceOfSDO(selfMinusOther);
        commSync->commRecvSequenceOfSDO(otherMinusSelf);
        mySyncStats.timerEnd(SyncStats::COMM_TIME);

        // print stats
        stringstream stats;
        stats << "selfMinusOther num = " << selfMinusOther.size() << endl;
        stats << "otherMinusSelf num = " << otherMinusSelf.size() << endl;
        Logger::gLog(Logger::METHOD, stats.str());

        // close
        commSync->commClose();

        // Record Stats
        mySyncStats.increment(SyncStats::XMIT, commSync->getXmitBytes());
        mySyncStats.increment(SyncStats::RECV, commSync->getRecvBytes());

        return true;
    }
    catch (SyncFailureException& e) {
        Logger::gLog(Logger::METHOD_DETAILS, e.what());
//        rethrow_exception(e);
        throw e;
    }
}

bool TrivialSync::SyncServer(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                             list<shared_ptr<DataObject>> &otherMinusSelf) {
    try {
        Logger::gLog(Logger::METHOD, "TSync::SyncServer");

        SyncMethod::SyncServer(commSync, selfMinusOther, otherMinusSelf);

        // connect
        mySyncStats.timerStart(SyncStats::IDLE_TIME);
        commSync->commListen();
        mySyncStats.timerEnd(SyncStats::IDLE_TIME);

        // destruct the adapter at the end of this range
        {
            // use adapter to "push_back" value to hashset
            struct SetAdapter {
                typedef shared_ptr<DataObject> value_type;
                multiset<shared_ptr<DataObject>, cmp<shared_ptr<DataObject>>> set;
//                unordered_multiset<shared_ptr<DataObject>> hashset;

                void push_back(const shared_ptr<DataObject> &data) {
                    set.emplace(data);
                }
            } setAdapter;

            // receive client data
            mySyncStats.timerStart(SyncStats::COMM_TIME);
            commSync->commRecvSequenceOfSDO(setAdapter);
//        mySyncStats.timerEnd(SyncStats::COMM_TIME);

            // calculate difference
            for (const auto &ele: myData) {
                auto it = setAdapter.set.find(ele);
                if (it == setAdapter.set.end()) // self - other
                    selfMinusOther.push_back(ele);
                else { // self union other
                    setAdapter.set.erase(it);
                }
            }
            for (auto &data: setAdapter.set)
                otherMinusSelf.push_back(data);
        }

//        mySyncStats.timerStart(SyncStats::COMM_TIME);
        commSync->commSendSequenceOfSDO(otherMinusSelf);
        commSync->commSendSequenceOfSDO(selfMinusOther);
        mySyncStats.timerEnd(SyncStats::COMM_TIME);

        // print stats
        stringstream stats;
        stats << "selfMinusOther num = " << selfMinusOther.size() << endl;
        stats << "otherMinusSelf num = " << otherMinusSelf.size() << endl;
        Logger::gLog(Logger::METHOD, stats.str());

        // close
        commSync->commClose();

        // Record Stats
        mySyncStats.increment(SyncStats::XMIT, commSync->getXmitBytes());
        mySyncStats.increment(SyncStats::RECV, commSync->getRecvBytes());

        return true;
    }
    catch (SyncFailureException& e) {
        Logger::gLog(Logger::METHOD_DETAILS, e.what());
        throw e;
    }
}

string TrivialSync::getName() {
    return "Trivial Sync";
}

bool TrivialSync::addElem(shared_ptr<DataObject> newDatum) {
    Logger::gLog(Logger::METHOD,"Entering TrivialSync::addElem");

    if(!SyncMethod::addElem(newDatum)) return false;
    myData.insert(newDatum);
    Logger::gLog(Logger::METHOD, "Successfully added shared_ptr<DataObject> {" + newDatum->print() + "}");
    return true;

}

bool TrivialSync::delElem(shared_ptr<DataObject> newDatum) {
    Logger::gLog(Logger::METHOD, "Entering TrivialSync::delElem");

    if(!SyncMethod::delElem(newDatum)) return false;
    myData.erase(newDatum);
    Logger::gLog(Logger::METHOD, "Successfully removed shared_ptr<DataObject> {" + newDatum->print() + "}");
    return true;
}

//pair<vector<string>, vector<string>> TrivialSync::getDiff(const string &a, const string &b) {
//    MyersDiff<string> diff(a, b);
//
//    auto stats = diff.stats();
//    pair<vector<string>, vector<string>> res;
//    res.first.reserve(stats.deleted);
//    res.first.reserve(stats.inserted);
//
//    for (const auto& d: diff) {
//        const auto& range = d.text;
//        switch (d.operation) {
//            case INSERT:
//                res.second.emplace_back(range.from, range.till);
//                break;
//            case DELETE:
//                res.first.emplace_back(range.from, range.till);
//                break;
//            default:
//                res.first.emplace_back();
//                break;
//        }
//    }
//    return res;
//}
