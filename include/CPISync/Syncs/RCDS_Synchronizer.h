//
// Created by bruce on 2/16/2023.
//

#ifndef CPISYNC_RCDS_SYNCHRONIZER_H
#define CPISYNC_RCDS_SYNCHRONIZER_H

#include <vector>
#include <string>
#include <unordered_map>
#include <thread>
#include <algorithm>
#include <NTL/ZZ.h>
#include <NTL/ZZ_p.h>

#include <CPISync/Aux_/SyncMethod.h>
#include <CPISync/Aux_/Auxiliary.h>
#include <CPISync/Aux_/SyncMethod.h>
#include <CPISync/Communicants/Communicant.h>
#include <CPISync/Data/DataObject.h>
#include <CPISync/Syncs/GenSync.h>

#include "StrataEst.h"
#include "InterCPISync.h"
#include "IBLTSync_SetDiff.h"
#include "ProbCPISync.h"
#include "FullSync.h"


using std::vector;
using std::hash;
using std::string;
using namespace NTL;

//typedef unsigned short sm_i; // small index for lvl, MAX = 65,535 which is terminal string size * partition^lvl = file string size

// A cycle structure, inspired by Bowen's Go implementation
struct cycle {
    size_t head;
    uint32_t len;
    uint32_t cyc;

    friend bool operator==(const cycle &a, const cycle &b) {
        return a.head == b.head and a.len == b.len and a.cyc == b.cyc;
    };

    friend bool operator!=(const cycle &a, const cycle &b) {
        return !(a == b);
    };

    friend ostream &operator<<(ostream &os, const cycle& rhs) {
        os << "Cycle: head: " + to_string(rhs.head) + ", len: " + to_string(rhs.len) + ", cyc: " + to_string(rhs.cyc) << endl;
        return os;
    };
};

// 重要: 需要熟记这个struct
struct shingle {
    size_t first;           // 1st part of this shingle
    size_t second;          // 2nd part of this shingle
    uint16_t level;         // level
    uint16_t occur_time;    // occurred time

    friend bool operator<(const shingle &a, const shingle &b) {
        return (a.first < b.first) ||
               (a.first == b.first and a.second < b.second) ||
               (a.first == b.first and a.second == b.second and a.occur_time < b.occur_time);
    };

    friend bool operator==(const shingle &a, const shingle &b) {
        return a.first == b.first and a.second == b.second and a.occur_time == b.occur_time and a.level == b.level;
    }

    friend  bool operator!=(const shingle &a, const shingle &b) {
        return !(a == b);
    };

    friend ostream &operator<<(ostream &os, const shingle shingle) {
        os << "Shingle: fst: " + to_string(shingle.first) + ", sec: " + to_string(shingle.second) + ", occ: " +
              to_string(shingle.occur_time) + ", lvl: " + to_string(shingle.level);
        return os;
    };
};


static vector<size_t> get_local_mins(const vector<size_t> &hash_val, size_t win_size) {
    // corner case
    if (win_size < 1) {
        Logger::gLog(Logger::METHOD, "Window size cannot < 1! Automatically set to 1.");
        win_size = 1;
    }

    // TODO: try predicting the size of mins
    vector<size_t> mins;

    // corner case
    if (2 * win_size + 1 > hash_val.size())
        return mins;

    // 将一个2 * win_size + 1窗口大小的hash值加入map
    // We should use RB tree to maintain order.
    map<size_t, size_t> hash_occur_cnt;
    for (size_t j = 0; j < 2 * win_size + 1; ++ j)
        ++ hash_occur_cnt[hash_val[j]];

    // i为窗口中心
    for (size_t i = win_size; i < hash_val.size() - win_size; ++ i) {
        // smaller than smallest hash && bigger than one window
        if (hash_val[i] <= hash_occur_cnt.begin()->first && ((mins.empty()) ? i : i - mins.back()) > win_size)
            mins.push_back(i);

        // 在最后一次循环跳出, 不然下一句会超出索引
        if (i + win_size + 1 == hash_val.size()) break;

        // 窗口最左边跟窗口最右边的右边的哈希值一样, 可以跳过删除最左边和插入最右边
        if (hash_val[i - win_size] == hash_val[i + win_size + 1]) continue;

        // 找窗口最左边的hash, 找到则计数减一或删去
        auto it_prev = hash_occur_cnt.find(hash_val[i - win_size]);
        if (it_prev != hash_occur_cnt.end()) {
            if (it_prev->second > 1) it_prev->second--;
            else hash_occur_cnt.erase(it_prev);
        }

        // 插入窗口最右边的hash
        ++ hash_occur_cnt[hash_val[i + win_size + 1]];
    }

    return mins;
}


class RCDS_Synchronizer {
public:
    // 这个构造, 在RCDS中没有调用
    // TODO: remove it
    RCDS_Synchronizer(size_t terminal_str_size, size_t levels, size_t partition, GenSync::SyncProtocol base_set_proto,
                      size_t shingle_size = 2, size_t ter_space = 8);

    RCDS_Synchronizer(GenSync::SyncProtocol base_set_proto, size_t levels, size_t partition, size_t terminal_str_size = 10)
            : terminalStrSz(terminal_str_size), level_num(levels), partition_num(partition),
              baseSyncProtocol(base_set_proto) {
        useExistingConnection = true;
        shingle_sz = 2;
        space_sz = 8;
    };

    ~RCDS_Synchronizer() = default;

    // 重建字符串
    string retrieve_string();

    // add one string, should be called only once in one instance
    bool addStr(shared_ptr<DataObject>& str, vector<shared_ptr<DataObject>> &datum);

    bool SyncClient(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                    list<shared_ptr<DataObject>> &otherMinusSelf, std::unordered_map<string, double> &CustomResult);

    bool SyncServer(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                    list<shared_ptr<DataObject>> &otherMinusSelf);

    static string getName() { return "Sets of Content"; }

    // 因为取消了多态, 所以要把它变为public
    // 为了适配vector, 从list改为了vector
    bool reconstructString(shared_ptr<DataObject>&recovered_string, const vector<shared_ptr<DataObject>> &mySetData);

private:
    bool useExistingConnection; // if use existing connection

    string data; // original data
    size_t terminalStrSz, level_num, partition_num, shingle_sz, space_sz;

    GenSync::SyncProtocol baseSyncProtocol;

    vector<shared_ptr<DataObject>> setPointers; // garbage collector

    vector<std::set<shingle>> hashShingleTree; // hash shingle tree

    map<size_t, pair<string, pair<size_t, size_t>>> dictionary;  // dictionary strings

    // origin, cycle information to reform this string rep
    unordered_map<size_t, vector<size_t>> cyc_dict; // has to be unique

    // requests
    unordered_map<size_t, string> term_concern, term_query;
    // should not use hashmap
    map<size_t, cycle> cyc_concern, cyc_query;

    /**
     * Create content dependent partitions based on the input string
     * Update Dictionary
     * @param str origin string to be partitioned
     * @param win_size partition size is 2 * win_size
     * @param space smaller-more partitions(hash space 论文有)
     * @param shingle_size inter-relation of the string
     * @return vector of substring hashes in origin string order
     */
    vector<size_t> create_hash_vec(size_t str_hash, size_t space = 0, size_t shingle_size = 0);


    /**
     * Insert string into dictionary
     * @param str a substring
     * @return hash of the string
     * @throw if there is duplicates, suggest using new/multiple hash functions
     */
    // 直接传入字符串进去
    size_t add_str_to_dictionary(const string &str) {
//        size_t hash = str_to_hash(str);
        size_t hash = std::hash<string>()(str);
        dictionary.emplace(hash, make_pair(str, make_pair(0, 0)));
//        if (!it.second and str != it.first->second.first and
//            str != myString.substr(it.first->second.second.first, it.first->second.second.second))
//            throw invalid_argument("Dictionary duplicated suggest using new/multiple hash functions");
        return hash;
    };

    // 通过索引放子字符串进去
    size_t add_i_to_dictionary(size_t start_i, size_t len) {
//        size_t hash = str_to_hash(myString.substr(start_i, len));
        size_t hash = std::hash<string>()(data.substr(start_i, len));
        dictionary.emplace(hash, make_pair("", make_pair(start_i, len)));
//        if (!it.second and myString.substr(it.first->second.second.first, it.first->second.second.second) != myString.substr(start_i, len) and
//            it.first->second.first != myString.substr(start_i, len))
//            throw invalid_argument("Dictionary duplicated suggest using new/multiple hash functions");
        return hash;
    };

    // 通过哈希找字符串
    string dict_getstr(size_t hash) {
        auto it = dictionary.find(hash);
        if (it != dictionary.end()) {
            if (it->second.first.empty() and it->second.second.second != 0)
                return data.substr(it->second.second.first, it->second.second.second);
            else return it->second.first;
        }
        return "";
    }

    // only available for local substrings
    // use it in partition tree construction
    // 返回对应的pair
    pair<size_t, size_t> dict_geti(size_t hash) {
        auto it = dictionary.find(hash);
        if (it != dictionary.end()) return it->second.second;

        return make_pair(0, 0);
    }

    // extract the unique substring hashes from the shingle_hash vector
    // 从一堆shingle_hash中得到去重的字符串哈希值
    vector<size_t> unique_substr_hash(std::set<shingle> hash_set) {
        std::set<size_t> tmp;
        for (shingle item : hash_set) {
            tmp.insert(item.first);
            tmp.insert(item.second);
        }
        return vector<size_t>(tmp.begin(), tmp.end());

    }

    // 使用提供的hash_vector, 将shingles写到树的某一层中
    void update_tree_shingles(vector<size_t> hash_vector, uint16_t level);

    // getting the poteintial list of next shingles
    vector<shingle>
    get_nxt_shingle_vec(const size_t cur_edge, const map<size_t, vector<shingle>> &last_state_stack,
                        const map<size_t, vector<shingle>> &original_state_stack);

    // functions for backtracking
    /**
     * peice shingle_hashes back to a vector of hashes in the string order
     * @param shingle_set
     * @param str_order
     * @param final_str a hash train in string order
     */
     // 将shingle_set唯一地转化为字符串
    bool shingle2hash_train(cycle &cyc_info, const std::set<shingle> &shingle_set, vector<size_t> &final_str);

    // 树 转化为 shingle.first->shingles, 并对shingles排序
    std::map<size_t, vector<shingle>> tree2shingle_dict(const std::set<shingle> &tree_lvl);

    // 返回原来的shingle, current_edge更新为shingle的second
    shingle get_nxt_edge(size_t &current_edge, shingle _shingle);

    /**
     *
     * @param shingle_hash_theirs
     * @param shingle_hash_mine
     * @param groupIDs
     * @return hashes of unknown
     */
    // 我没有的对方有的
    void prepare_querys(list<shared_ptr<DataObject>> &shingle_hash_theirs);

//    void prepare_concerns(const vector<shingle_hash> &shingle_hash_theirs, const vector<shingle_hash> &shingle_hash_mine);

    // 回答对方的query
    bool answer_queries(std::set<size_t> &theirQueries);

    // 每一层使用update_tree_shingles更新一遍(包括第0层, 即整个字符串)
    void go_through_tree();

    // functions for Sync Methods


    //Get fuzzy_shingle in ZZ O(n*log^2(n))
    // 获取myTree中去重的ZZ值
    vector<ZZ> getHashShingles_ZZ() {
        std::set<ZZ> res;
        for (auto& treelvl : hashShingleTree) {
            for (auto item:treelvl) {
                res.emplace(TtoZZ(item));
            }
        }
        return vector<ZZ>{res.begin(), res.end()};
    };

    // 获取myTree中的数量
    size_t getNumofTreeNodes() {
        size_t
                num_treenodes = 0;
        for (auto& lvl : hashShingleTree) num_treenodes += lvl.size();
        return num_treenodes;
    }

    void SendSyncParam(const shared_ptr<Communicant> &commSync, bool oneWay = false);

    void RecvSyncParam(const shared_ptr<Communicant> &commSync, bool oneWay = false);

    void configure(shared_ptr<SyncMethod> &setHost, long mbar, size_t elem_size);

    bool setReconClient(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                        vector<shared_ptr<DataObject>> &full_set, list<shared_ptr<DataObject>> &selfMinusOther,
                        list<shared_ptr<DataObject>> &otherMinusSelf) {
        selfMinusOther.clear();
        otherMinusSelf.clear();
//        cout << "Client Size: " << full_set.size();
        shared_ptr<SyncMethod> setHost;
        // 取消多态, 抽出这句有用的出来
        commSync->resetCommCounters();
//        SyncMethod::SyncClient(commSync, selfMinusOther, otherMinusSelf);
        configure(setHost, mbar, elem_size);
        for (auto& dop : full_set) {
            setHost->addElem(dop); // Add to GenSync
        }
        bool success = setHost->SyncClient(commSync, selfMinusOther, otherMinusSelf);
//        if (not SyncMethod::delGroup(full_set, selfMinusOther))
        if (not delGroup(full_set, selfMinusOther))
            Logger::error_and_quit("We failed to delete some set elements");
        for (auto item : otherMinusSelf)
            full_set.push_back(item);

//        cout << " with sym Diff: " << selfMinusOther.size() + otherMinusSelf.size() << " After Sync at : "
//             << full_set.size() << endl;

        return success;
    };

    bool setReconServer(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                        vector<shared_ptr<DataObject>> &full_set, list<shared_ptr<DataObject>> &selfMinusOther,
                        list<shared_ptr<DataObject>> &otherMinusSelf) {
        selfMinusOther.clear();
        otherMinusSelf.clear();
//        cout << "Server Size: " << full_set.size();
        shared_ptr<SyncMethod> setHost;
        // 取消多态, 抽出这句有用的出来
        commSync->resetCommCounters();
//        SyncMethod::SyncServer(commSync, selfMinusOther, otherMinusSelf);
        configure(setHost, mbar, elem_size);
        for (auto& dop : full_set) {
            setHost->addElem(dop); // Add to GenSync
        }
        return setHost->SyncServer(commSync, selfMinusOther, otherMinusSelf);

//        cout << " with sym Diff: " << selfMinusOther.size() + otherMinusSelf.size() << " After Sync at : "
//             << full_set.size() << endl;
    };

public:
    /**
     * @author Bruce Xin
     * Get rid of del list from a group in O(d+nlgd+(d))
     * 也给外部用的工具方法
     * @param itemGroup n num of elem
     * @param delList d num of elem
     * @param destroy Destructively delete all delList
     * @return
     */
    static bool delGroup(vector<shared_ptr<DataObject>> &itemGroup, list<shared_ptr<DataObject>> &delList) {

        // d+nlgd
        std::set<ZZ> delMap;
        for (auto& item : delList)
            delMap.insert(item->to_ZZ());

        auto it = itemGroup.begin();
        while (it != itemGroup.end()) {
            auto delit = delMap.find((*it)->to_ZZ());
            if (delit != delMap.end()) {
                it = itemGroup.erase(it); // delete the current and move to the next
                delMap.erase(delit);
            } else it++;
        }

        return (delMap.size() == 0);
    }
};

#endif //CPISYNC_RCDS_SYNCHRONIZER_H
