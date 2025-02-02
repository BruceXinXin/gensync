/* This code is part of the CPISync project developed at Boston University.  Please see the README for use and references. */
/*
 * File:   CommunicantTest.cpp
 * Author: Eliezer Pearl
 *
 * Created on May 24, 2018, 10:08:52 AM
 */

#include "CommunicantTest.h"
#include <CPISync/Communicants/CommDummy.h>

CPPUNIT_TEST_SUITE_REGISTRATION(CommunicantTest);

CommunicantTest::CommunicantTest() = default;
CommunicantTest::~CommunicantTest() = default;

void CommunicantTest::setUp() {
    const int MY_SEED = 617; // a preset seed for pseudorandom number generation
    srand(MY_SEED); // seed the prng predictably so that the random numbers generated are predictable and reproducible
}

void CommunicantTest::tearDown() {
}

void CommunicantTest::testConstruct() {
    try {
        queue<char> qq;
        CommDummy c(&qq); // since every constructor calls their parent's empty constructors, we effectively test Communicant's empty constructor
    } catch(...) {
        CPPUNIT_FAIL("Expected no exceptions.");
    }
    
    // no exceptions thrown
    CPPUNIT_ASSERT(true);
}

void CommunicantTest::testBytesAndResetCommCounters() {
    queue<char> qq;
    
    // set up two communicants to either send or receive
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);
    
    unsigned long expXmitTot = 0;
    unsigned long expRecvTot = 0;
    
    for(int ii = 0; ii < TIMES; ii++) {
        string toSend = randString(LOWER_BOUND, UPPER_BOUND);
        size_t strLength = toSend.length();
        
        CPPUNIT_ASSERT_EQUAL(expXmitTot, cSend.getXmitBytesTot());
        CPPUNIT_ASSERT_EQUAL(expRecvTot, cRecv.getRecvBytesTot());
        
        cSend.resetCommCounters();
        cRecv.resetCommCounters();
        
        CPPUNIT_ASSERT_EQUAL(0ul, cSend.getXmitBytes());
        CPPUNIT_ASSERT_EQUAL(0ul, cRecv.getRecvBytes());
        
        cSend.commSend(toSend.data(), strLength);
        
        expXmitTot += strLength;
        
        CPPUNIT_ASSERT_EQUAL((unsigned long) strLength, cSend.getXmitBytes());
        CPPUNIT_ASSERT_EQUAL(expXmitTot, cSend.getXmitBytesTot());
        
        cRecv.commRecv(strLength);
        
        expRecvTot += strLength;
        
        CPPUNIT_ASSERT_EQUAL((unsigned long) strLength, cRecv.getRecvBytes());
        CPPUNIT_ASSERT_EQUAL(expRecvTot, cRecv.getRecvBytesTot());
    }
}

void CommunicantTest::testEstablishModAndCommZZ_p() {
    queue<char> qq;
    
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);
    
    for(int ii = 0; ii < TIMES; ii++) {
        ZZ_p::init(randZZ());
    
        // Tests that establishMod works with oneWay set to true.
        CPPUNIT_ASSERT(cSend.establishModSend(true));
        CPPUNIT_ASSERT(cRecv.establishModRecv(true));
        
        ZZ_p exp(rand());
        cSend.commSend(exp);
        
        CPPUNIT_ASSERT_EQUAL(exp, cRecv.commRecv_ZZ_p());
    }
}

void CommunicantTest::testCommUstringBytes() {
    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);
    
    for(int ii = 0; ii < TIMES; ii++) {
        const ustring exp = (const unsigned char *) (randString(LOWER_BOUND, UPPER_BOUND).data());
        size_t expLen = exp.size();

        // check that transmitted bytes are successfully incremented 
        const unsigned long before = cSend.getXmitBytes();
        cSend.Communicant::commSend(exp, expLen);
        const unsigned long after = cSend.getXmitBytes();

        CPPUNIT_ASSERT_EQUAL(0, exp.compare(cRecv.commRecv_ustring(expLen)));
        CPPUNIT_ASSERT_EQUAL(before + expLen, after);
    }
}

void CommunicantTest::testCommUstringNoBytes() {
    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);
    
    for(int ii = 0; ii < TIMES; ii++) {
        const ustring exp = (const unsigned char *) (randString(LOWER_BOUND, UPPER_BOUND).data());
        cSend.Communicant::commSend(exp);
    
        CPPUNIT_ASSERT_EQUAL(0, exp.compare(cRecv.commRecv_ustring()));
    }
}

void CommunicantTest::testCommString() {
    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);

    for(int ii = 0; ii < TIMES; ii++) {
        const string exp = randString(LOWER_BOUND, UPPER_BOUND);
        cSend.Communicant::commSend(exp);
        
        CPPUNIT_ASSERT_EQUAL(exp, cRecv.commRecv_string());
    }
}

void CommunicantTest::testCommLong() {
    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);

    for(int ii = 0; ii < TIMES; ii++){
        const long exp = randLong();
        cSend.Communicant::commSend(exp);
        CPPUNIT_ASSERT_EQUAL(exp, cRecv.commRecv_long());
    }
}

void CommunicantTest::testCommDataObject() {
    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);

    for(int ii = 0; ii < TIMES; ii++){
        DataObject exp(randString(LOWER_BOUND, UPPER_BOUND));
        cSend.Communicant::commSend(exp);
        
        CPPUNIT_ASSERT_EQUAL(exp.to_string(), cRecv.commRecv_DataObject()->to_string());
    }
}

void CommunicantTest::testCommDataObjectPriority() { // fix this test so that the repisint doesnt need to be changed
    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);

    // Test case where RepIsInt is true
    DataObject::RepIsInt = true;

    for(int ii = 0; ii < TIMES; ii++) {

        DataPriorityObject exp(static_cast<clock_t>(randLong()));
        exp.setPriority(randZZ());

        cSend.Communicant::commSend(exp);
        DataPriorityObject* res = cRecv.commRecv_DataObject_Priority();

        CPPUNIT_ASSERT_EQUAL(exp.to_string(), res->to_string());
        CPPUNIT_ASSERT_EQUAL(exp.getPriority(), res->getPriority());
    }

    // Test case where RepIsInt is false
    DataObject::RepIsInt = false;

    for(int ii = 0; ii < TIMES; ii++) {
        DataPriorityObject exp(static_cast<clock_t>(randLong()));
        exp.setPriority(randZZ());

        cSend.Communicant::commSend(exp);
        DataPriorityObject* res = cRecv.commRecv_DataObject_Priority();

        CPPUNIT_ASSERT_EQUAL(exp.to_string(), res->to_string());
        CPPUNIT_ASSERT_EQUAL(exp.getPriority(), res->getPriority());
    }
}

void CommunicantTest::testCommDataObjectList() {
    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);

    const int TIMES = 50;
    const int LOWER_BOUND = 1;
    const int UPPER_BOUND = 10;

    for(int ii = 0; ii < TIMES; ii++){
        int length = randLenBetween(LOWER_BOUND, UPPER_BOUND);

        list<shared_ptr<DataObject>> exp;
        for(int jj = 0; jj < length; jj++) {
            shared_ptr<DataObject> dd = make_shared<DataObject>(randZZ());
            exp.push_back(dd);
        }

        cSend.commSend(exp);
        const list<shared_ptr<DataObject>> res = cRecv.commRecv_DoList();
        // assert same length before iterating to check their equality
        CPPUNIT_ASSERT_EQUAL(exp.size(), res.size());

        list<shared_ptr<DataObject>>::const_iterator expI = exp.begin();
        auto resI = res.begin();

		//Memory is deallocated here because these are shared_ptrs and are deleted when the last ptr to an object is deleted
		exp.clear();
    }
}

void CommunicantTest::testCommDouble() {
    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);

    // lower bound and upper bound for doubles; overrides the global constants
    const double LOWER_BOUND = 0;
    const double UPPER_BOUND = 5000;
    for(int ii = 0; ii < TIMES; ii++) {
        const double exp = randDouble(LOWER_BOUND, UPPER_BOUND);

        cSend.Communicant::commSend(exp);
   
        CPPUNIT_ASSERT_DOUBLES_EQUAL(exp, cRecv.commRecv_double(), DELTA);
    }
}

void CommunicantTest::testCommByte() {
    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);
    
    for(int ii = 0; ii < TIMES; ii++) {
        const byte exp = randByte();
        cSend.Communicant::commSend(exp);

        CPPUNIT_ASSERT_EQUAL(exp, cRecv.commRecv_byte());
    }
}

void CommunicantTest::testCommInt() {
    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);
    
    for(int ii = 0; ii < TIMES; ii++) {
        const int exp = rand();
        cSend.Communicant::commSend(exp);

        CPPUNIT_ASSERT_EQUAL(exp, cRecv.commRecv_int());
    }
}

void CommunicantTest::testCommVec_ZZ_p() {
    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);
    
    for(int ii = 0; ii < TIMES; ii++) {
        ZZ_p::init(randZZ());
    
        // Tests that establishMod works with oneWay set to true.
        cSend.establishModSend(true);
        cRecv.establishModRecv(true);
        
        // pick a length in between lower and upper, inclusive
        int length = randLenBetween(LOWER_BOUND, UPPER_BOUND);
        
        vec_ZZ_p exp;
        for(int jj = 0; jj < length; jj++)
            exp.append(random_ZZ_p());
        
        cSend.Communicant::commSend(exp);
        
        CPPUNIT_ASSERT_EQUAL(exp, cRecv.commRecv_vec_ZZ_p());
    }
}

void CommunicantTest::testCommZZ() {
    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);

    for(int ii = 0; ii < TIMES; ii++) {
        const ZZ exp = randZZ();
        const int expSize = sizeof(exp);
        cSend.Communicant::commSend(exp, expSize);

        CPPUNIT_ASSERT_EQUAL(exp, cRecv.commRecv_ZZ(expSize));
    }
}

void CommunicantTest::testCommZZNoArgs(){
    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);

    for(int ii = 0; ii < TIMES; ii++) {
        const ZZ exp = randZZ();

        cSend.Communicant::commSend(exp);

        CPPUNIT_ASSERT_EQUAL(exp, cRecv.commRecv_ZZ());
    }
}

void CommunicantTest::testCommSequence() {
    static constexpr int seq_len = 1e2;

    queue<char> qq;
    CommDummy cSend(&qq);
    CommDummy cRecv(&qq);

    for (int i = 0; i < TIMES; ++i) {
        vector<shared_ptr<DataObject>> objs, recv;
        objs.reserve(seq_len);
        for (int j = 0; j < seq_len; ++j) {
            objs.emplace_back(make_shared<DataObject>(randString(0, seq_len)));
        }
        cSend.Communicant::commSendSequenceOfSDO(objs);
        cRecv.Communicant::commRecvSequenceOfSDO(recv);
        for (int j = 0; j < seq_len; ++j) {
            CPPUNIT_ASSERT_EQUAL(*objs[j], *recv[j]);
        }
    }
}
