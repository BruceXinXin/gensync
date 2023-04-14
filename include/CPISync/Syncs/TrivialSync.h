//
// Created by bruce on 2/5/2023.
//

#ifndef CPISYNC_TRIVIALSYNC_H
#define CPISYNC_TRIVIALSYNC_H

#include <unordered_set>

#include <CPISync/Aux_/SyncMethod.h>
#include <CPISync/Communicants/Communicant.h>
#include <CPISync/Data/DataObject.h>

class TrivialSync: public SyncMethod {
public:
    TrivialSync();
    ~TrivialSync() override;

    bool SyncClient(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                    list<shared_ptr<DataObject>> &otherMinusSelf) override;

    bool SyncServer(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                    list<shared_ptr<DataObject>> &otherMinusSelf) override;

    bool addElem(shared_ptr<DataObject> newDatum) override;

    bool delElem(shared_ptr<DataObject> newDatum) override;

    string getName() override;

private:
    // Note: we cannot use hashset because we need to keep the order
    multiset<shared_ptr<DataObject>, cmp<shared_ptr<DataObject>>> myData;
//    unordered_multiset<shared_ptr<DataObject>> myData;
//public:
//    pair<vector<string>, vector<string>> getDiff(const string &a, const string &b);
};

#endif // CPISYNC_TRIVIALSYNC_H
