//
// Created by bruce on 3/10/2023.
//

#ifndef CPISYNC_RCDSTEST_H
#define CPISYNC_RCDSTEST_H

#include <cppunit/extensions/HelperMacros.h>

/**
* 思路: 传两种协议 和 文件或文件夹, 一共四种组合
*/

class RCDSTest : public CPPUNIT_NS::TestFixture {
    CPPUNIT_TEST_SUITE(RCDSTest);

    CPPUNIT_TEST(RCDSSetReconcileTest);

    CPPUNIT_TEST_SUITE_END();
public:
    RCDSTest() = default;

    ~RCDSTest() override = default;
    void setUp() override;
    void tearDown() override {}

    /**
     * Test full reconciliation of a set with RCDS protocol (All elements are exchanged and the sets are updated
    * to be the Union of the two sets
     */
    void RCDSSetReconcileTest();

private:
    void testAll();
};

#endif //CPISYNC_RCDSTEST_H

