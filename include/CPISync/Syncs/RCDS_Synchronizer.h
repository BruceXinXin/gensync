//
// Created by bruce on 2/16/2023.
//

#ifndef CPISYNC_RCDS_SYNCHRONIZER_H
#define CPISYNC_RCDS_SYNCHRONIZER_H

#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
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

//#include "StrataEst.h"
#include "InterCPISync.h"
#include "IBLTSync_SetDiff.h"
#include "ProbCPISync.h"
#include "FullSync.h"

using std::vector;
using std::hash;
using std::string;
using namespace NTL;

// A cycle structure, inspired by Bowen's Go implementation
struct cycle {
    size_t head;
    uint32_t len;
    uint32_t cyc; // cycle number

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
        if (hash_val[i] <= hash_occur_cnt.begin()->first && ((mins.empty())? i : i - mins.back()) > win_size)
            mins.push_back(i);

        // 在最后一次循环跳出, 不然下一句会超出索引
        if (i + win_size + 1 == hash_val.size())
            break;

        // 窗口最左边跟窗口最右边的右边的哈希值一样, 可以跳过删除最左边和插入最右边
        if (hash_val[i - win_size] == hash_val[i + win_size + 1])
            continue;

        // 找窗口最左边的hash, 找到则计数减一或删去
        auto it_prev = hash_occur_cnt.find(hash_val[i - win_size]);
        if (it_prev != hash_occur_cnt.end()) {
            if (it_prev->second > 1)
                -- it_prev->second;
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
            : terminal_str_size(terminal_str_size), level_num(levels), partition_num(partition),
              base_sync_protocol(base_set_proto) {
        useExistingConnection = true;
        shingle_sz = 2;
        space_sz = 8;
    };

    ~RCDS_Synchronizer() = default;

    // 重建字符串
    string retrieve_string();

    // add one string, should be called only once in one instance
    bool add_str(shared_ptr<DataObject>& str);

    bool SyncClient(const shared_ptr<Communicant> &commSync);

    bool SyncServer(const shared_ptr<Communicant> &commSync);

    static string getName() { return "RCDS_Synchronizer"; }

    bool recover_str(shared_ptr<DataObject>& recovered_str);

private:
    bool useExistingConnection; // if use existing connection

    string data; // original data
    size_t terminal_str_size, level_num, partition_num, shingle_sz, space_sz;

    GenSync::SyncProtocol base_sync_protocol;

    vector<shared_ptr<DataObject>> set_pointers; // garbage collector

    vector<std::set<shingle>> hash_shingle_tree; // hash shingle tree

    // hash -> (string, (start_idx, len))
    unordered_map<size_t, pair<string, pair<size_t, size_t>>> dictionary;  // dictionary strings

    // origin, cycle information to reform this string rep
    unordered_map<size_t, vector<size_t>> cyc_dict; // has to be unique

    // requests
    unordered_map<size_t, string> term_concern, term_query;
    // should not use hashmap
    map<size_t, cycle> cyc_concern, cyc_query;

private:
    /**
     * Create content dependent partitions based on the input string
     * Update Dictionary
     * @param str_hash the hash of the string
     * @param space smaller-more partitions(hash space 论文有)
     * @param shingle_size inter-relation of the string
     * @return vector of substring hashes in origin string order
     */
    vector<size_t> create_hash_vec(size_t str_hash, size_t space = 0, size_t shingle_size = 0);

    // 每一层使用update_tree_shingles更新一遍(包括第0层, 即整个字符串)
    void init_tree_by_string_data();

    /**
     * Generate queries
     * @param otherMinusSelf
     */
    // 我没有的对方有的
    void gen_queries(const list<shared_ptr<DataObject>> &otherMinusSelf);

    // 回答对方的query
    bool answer_queries(std::unordered_set<size_t> &queries);

    // 使用提供的hash_vector, 将shingles写到树的某一层中
    // shingle是{a点，b点，层数编号，出现次数}，a到b有连接
    void update_tree_shingles_by_level(const vector<size_t>& hash_vector, uint16_t level);

    // 得到所有可能的下一个shingle
    static vector<shingle> potential_next_shingles(size_t edge,
                                            const map<size_t, vector<shingle>> &cur,
                                            const map<size_t, vector<shingle>> &org);

    /**
     * shingle_hashes back to a vector of hashes in the string order
     * @param tree_level
     * @param str_order
     * @param hashes_vec a hash train in string order
     */
     // 将shingle_set唯一地转化为子字符串的hashes
    bool shingles_to_substr_hashes(cycle &cyc_info, int level, vector<size_t> &hashes_vec) const;

    // 树 转化为 shingle.first->shingles, 并对shingles排序
    std::map<size_t, vector<shingle>> tree_level_to_shingle_dict(int level) const;

//    // 返回原来的shingle, current_edge更新为shingle的second
//    shingle get_nxt_edge(size_t &current_edge, shingle _shingle);
//    size_t get_next_edge(const shingle& shingle);

    void send_sync_param(const shared_ptr<Communicant> &commSync) const;

    void recv_sync_param(const shared_ptr<Communicant> &commSync) const;

    void configure(shared_ptr<SyncMethod> &setHost, long mbar, size_t elem_size);

    /**
     * Insert string into dictionary
     * @param str a substring
     */
    // 直接传入字符串进去
    size_t add_str_to_dict(const string &str) {
        size_t hash = std::hash<string>()(str);
        dictionary.emplace(hash, make_pair(str, make_pair(0, 0)));
        return hash;
    };

    // 通过索引放子字符串进去
    size_t add_str_to_dict_by_idx_len(size_t idx, size_t len) {
        size_t hash = std::hash<string>()(data.substr(idx, len));
        dictionary.emplace(hash, make_pair("", make_pair(idx, len)));
        return hash;
    };

    // 通过哈希找字符串
    string find_str_from_dict_by_hash(size_t hash) {
        auto it = dictionary.find(hash);
        if (it != dictionary.end()) {
            if (it->second.first.empty() and it->second.second.second != 0)
                return data.substr(it->second.second.first, it->second.second.second);
            else
                return it->second.first;
        }
        return "";
    }

    // only available for local substrings
    // 返回对应的pair
    pair<size_t, size_t> get_idx_len_by_hash(size_t hash) {
        auto it = dictionary.find(hash);
        if (it != dictionary.end())
            return it->second.second;

        return {0, 0};
    }

    // 从一堆shingle_hash中得到去重的字符串哈希值, 返回值需要有序
    static vector<size_t> get_unique_hashes_from_shingles(const std::set<shingle>& hash_set) {
        std::set<size_t> tmp;
        for (const shingle& item : hash_set) {
            tmp.insert(item.first);
            tmp.insert(item.second);
        }
        return vector<size_t>(tmp.begin(), tmp.end());
    }

    // 获取myTree中去重的ZZ值
    vector<ZZ> get_unique_shingleZZs_from_tree() {
        std::set<ZZ> res;
        for (const auto& tree_level: hash_shingle_tree) {
            for (const auto& item: tree_level)
                res.emplace(TtoZZ(item));
        }
        return { res.begin(), res.end() };
    };

    bool setReconClient(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                        list<shared_ptr<DataObject>> &selfMinusOther, list<shared_ptr<DataObject>> &otherMinusSelf);

    bool setReconServer(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                        list<shared_ptr<DataObject>> &selfMinusOther, list<shared_ptr<DataObject>> &otherMinusSelf);

    /**
     * @param to_del elements to delete
     * @param destroy Destructively delete all delList
     * @return
     */
    bool del_elements_from_set_pointers(const list<shared_ptr<DataObject>> &to_del);
};

#endif //CPISYNC_RCDS_SYNCHRONIZER_H
