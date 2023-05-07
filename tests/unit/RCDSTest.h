//
// Created by bruce on 3/10/2023.
//

#ifndef CPISYNC_RCDSTEST_H
#define CPISYNC_RCDSTEST_H

#include <string>

#include <cppunit/extensions/HelperMacros.h>
#include <CPISync/Aux_/Logger.h>

using std::string;

/**
* 思路: 传两种协议 和 文件或文件夹, 一共四种组合
*/
class RCDSTest : public CPPUNIT_NS::TestFixture {
    CPPUNIT_TEST_SUITE(RCDSTest);

    CPPUNIT_TEST(RCDSSetReconcileTest);
//    CPPUNIT_TEST(BenchMark);

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

    void BenchMark();

private:
    void testAll();

};

class StringUtils {
public:
    static string rand_str(size_t len = 10);

    static bool rand_edit(string& str, size_t upper);

    static string delete_char(string str, char c);

private:
    static void string_delete(string& str);

    static void string_insert(string& str);

    static void string_modify(string& str);

};

class RSYNC_Test {
private:
    struct rsync_stats {
        long long xmit, recv;
        double time;
    };
public:
    static rsync_stats getRsyncStats(string origin, string target, bool full_report = false) {
        // only works for one type of rsync outputs
        rsync_stats stats;

        string res = subprocess_commandline(("rsync -r --checksum --no-whole-file --progress --stats " + origin + " " +
                                             target).c_str());  // -a archive -z compress -v for verbose
        try {
            stats.time = stod (StringUtils::delete_char(extractStringIn(res, "File list generation time: ", "seconds"), ','));
            stats.time += stod(StringUtils::delete_char(extractStringIn(res, "File list transfer time: ", "seconds"), ','));
            stats.xmit = stoll(StringUtils::delete_char(extractStringIn(res, "Total bytes sent: ", "\n"), ','));
            stats.recv = stoll(StringUtils::delete_char(extractStringIn(res, "Total bytes received: ", "\n"), ','));
        } catch (const std::exception &exc) {
            stats = {0, 0, 0};
        }
        if (full_report)
            cout << res << endl;
        return stats;
    }

private:
    static string extractStringIn(string org, string from, string to);

    static string subprocess_commandline(const char *command);
};

#endif //CPISYNC_RCDSTEST_H

