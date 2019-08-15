/* This code is part of the CPISync project developed at Boston University.  Please see the README for use and references. */

/* 
 * File:   FullSync.h
 * Author: Eliezer Pearl
 *
 * A class that stores and reconciles collections of data. Unlike CPISync, which
 * efficiently reconciles multi-sets, FullSync simply exchanges entire collections
 * from one peer to another for reconciliation.
 *
 * Created on June 27, 2018, 9:37 AM
 */

#ifndef FULLSYNC_H
#define FULLSYNC_H

#include <NTL/RR.h>
#include <NTL/ZZ_pX.h>
#include <NTL/vec_ZZ_p.h>
#include <NTL/ZZ_pXFactoring.h>
#include <algorithm>
#include <CPISync/Aux/Auxiliary.h>
#include <CPISync/Aux/SyncMethod.h>
#include <CPISync/Aux/Exceptions.h>

// namespaces
using namespace NTL;

class FullSync : public SyncMethod {
public:
    
    // General class constructor
    FullSync();
    
    // General class destructor
    ~FullSync() override;
  
    
    // parent methods to override
    bool SyncClient(const shared_ptr<Communicant>& commSync, list<shared_ptr<DataObject>> &selfMinusOther, list<shared_ptr<DataObject>> &otherMinusSelf) override;
    bool SyncServer(const shared_ptr<Communicant>& commSync, list<shared_ptr<DataObject>> &selfMinusOther, list<shared_ptr<DataObject>> &otherMinusSelf) override;
    bool addElem(shared_ptr<DataObject> newDatum) override;
    bool delElem(shared_ptr<DataObject> newDatum) override;
    inline string getName() override { return "Full Sync"; }

      /**
     * Deal with elements in OtherMinusSelf after finishing a specific sync function.
     * Works only when data type for elements is SET
     * @param *add function pointer to the addElem function in GenSync class
     * @param *del function pointer to the delElem function in GenSync class
     * @param otherMinusSelf list of dataObjects, received from every specific sync function
     * @param myData list of dataObjects, containing all elems saved in the data structure
     **/
    template <class T>
    static void postProcessing_SET(list<shared_ptr<DataObject>> otherMinusSelf, list<shared_ptr<DataObject>> myData, void (T::*add)(shared_ptr<DataObject>), bool (T::*del)(shared_ptr<DataObject>), T *pGenSync)
    {
        for (auto elem : otherMinusSelf)
        {
        (pGenSync->*add)(elem);
        }
    }


    /**
     * @return A string representing the elements stored in the FullSync object.
     */
    string printElem();
private:
    multiset<shared_ptr<DataObject>, cmp<shared_ptr<DataObject>>> myData;
};

#endif /* FULLSYNC_H */

