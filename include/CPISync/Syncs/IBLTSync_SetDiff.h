//
// Created by Bowen Song on 10/14/18.
//

#ifndef CPISYNCLIB_IBLTSYNC_SETDIFF_H
#define CPISYNCLIB_IBLTSYNC_SETDIFF_H

#include <CPISync/Aux_/SyncMethod.h>
#include <CPISync/Aux_/Auxiliary.h>
#include "IBLT.h"

// 暂时不把它加入GenSync.cpp中
class IBLTSync_SetDiff : public SyncMethod {
public:
    // Constructors and destructors
    IBLTSync_SetDiff(size_t expected_diff, size_t eltSize, bool keep_alive=false);
    ~IBLTSync_SetDiff();

    // Implemented parent class methods
    bool SyncClient(const shared_ptr<Communicant>& commSync, list<shared_ptr<DataObject>> &selfMinusOther, list<shared_ptr<DataObject>> &otherMinusSelf) override;
    bool SyncServer(const shared_ptr<Communicant>& commSync, list<shared_ptr<DataObject>> &selfMinusOther, list<shared_ptr<DataObject>> &otherMinusSelf) override;
    bool addElem(shared_ptr<DataObject> datum) override;
    bool delElem(shared_ptr<DataObject> datum) override;
    string getName() override;
protected:
    bool oneWay, keepAlive;
private:
    IBLT myIBLT;
    size_t expNumDiff;
};

#endif //CPISYNCLIB_IBLTSYNC_SETDIFF_H
