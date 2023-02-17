//
// Created by bruce on 2/7/2023.
//

#ifndef CPISYNC_TRIVIALSYNCTEST_H
#define CPISYNC_TRIVIALSYNCTEST_H

#include <cppunit/extensions/HelperMacros.h>

class TrivialSyncTest : public CPPUNIT_NS::TestFixture {
    CPPUNIT_TEST_SUITE(TrivialSyncTest);

    CPPUNIT_TEST(TrivialSyncSetReconcileTest);
    CPPUNIT_TEST(TrivialSyncMultisetReconcileTest);
    CPPUNIT_TEST(TrivialSyncLargeSetReconcileTest);

    CPPUNIT_TEST_SUITE_END();
public:
    TrivialSyncTest();

    ~TrivialSyncTest() override;
    void setUp() override;
    void tearDown() override;

    /**
     * Test full reconciliation of a set with TrivialSync protocol (All elements are exchanged and the sets are updated
    * to be the Union of the two sets
     */
    void TrivialSyncSetReconcileTest();

    /**
     * Test full reconciliation of a multiset with TrivialSync protocol (All elements are exchanged and the sets are
     * updated to be the Union of the two sets
     */
    void TrivialSyncMultisetReconcileTest();

    /**
    * Test full reconciliation of a set with TrivialSync protocol (All elements are exchanged and the sets are updated
    * to be the Union of the two sets
    */
    void TrivialSyncLargeSetReconcileTest();
};


#endif //CPISYNC_TRIVIALSYNCTEST_H
