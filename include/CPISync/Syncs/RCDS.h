//
// Created by bruce on 2/16/2023.
//

#ifndef CPISYNC_RCDS_H
#define CPISYNC_RCDS_H

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

#include "InterCPISync.h"
#include "ProbCPISync.h"
#include "FullSync.h"

using std::vector;
using std::hash;
using std::string;
using namespace NTL;

/**
 * A cycle structure, inspired by Bowen's Go implementation.
 *
 * head represents the start hash value, indicates where we should start backtracking
 * len represents the length of the path
 * backtracking_time represents how many times should we backtrack.
 *      With this param, given the same de Bruijn digraph,
 *      since we know how many times to backtrack to get the answer, we can uniquely get the string.
 */
struct cycle {
    size_t head;
    uint32_t len;
    uint32_t backtracking_time;

    friend bool operator==(const cycle &a, const cycle &b) {
        return a.head == b.head and a.len == b.len and a.backtracking_time == b.backtracking_time;
    };

    friend bool operator!=(const cycle &a, const cycle &b) {
        return !(a == b);
    };

    friend ostream &operator<<(ostream &os, const cycle& rhs) {
        os << "Cycle: head: " + to_string(rhs.head) + ", len: " + to_string(rhs.len) + ", backtracking_time: " + to_string(rhs.backtracking_time) << endl;
        return os;
    };
};

/**
 * An edge from first to second.
 */
struct shingle {
    size_t first;           // 1st part of this shingle
    size_t second;          // 2nd part of this shingle
    uint16_t level;         // level
    uint16_t occur_time;    // occurred time(represents multiple the same edges)

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
        os << "Shingle: first: " + to_string(shingle.first) + ", second: " + to_string(shingle.second) + ", occur_time: " +
              to_string(shingle.occur_time) + ", level: " + to_string(shingle.level);
        return os;
    };
};


class RCDS_Synchronizer {
public:
    // TODO: remove it
//    RCDS_Synchronizer(size_t terminal_str_size, size_t levels, size_t partition, GenSync::SyncProtocol base_set_proto,
//                      size_t shingle_size = 2, size_t ter_space = 8);

    RCDS_Synchronizer(GenSync::SyncProtocol base_set_proto, size_t levels, size_t partition, size_t terminal_str_size = 10)
            : terminal_str_size(terminal_str_size), level_num(levels), partition_num(partition),
              base_sync_protocol(base_set_proto) {
        use_existing_connection = true;
        shingle_sz_divisor = 2;
        space_sz_divisor = 8;
    };

    ~RCDS_Synchronizer() = default;

    // add one string, should be called only once in one instance
    // add rvalue override to avoid unnecessary data copy
    bool add_str(const string& str);
    bool add_str(string&& str);

    /**
     * Sync method for client
     * @param commSync: communicant for syncing
     * @return if success
     */
    bool SyncClient(const shared_ptr<Communicant> &commSync);

    /**
     * Sync method for server
     * @param commSync: communicant for syncing
     * @return if success
     */
    bool SyncServer(const shared_ptr<Communicant> &commSync);

    /**
     * Recover the string, method for client
     * @param recovered_str
     * @warning must be called after Syncing
     * @return
     */
    bool recover_str(shared_ptr<DataObject>& recovered_str);

private:
    // TODO: we no longer need it
    // if use existing connection
    bool use_existing_connection;

    // original data
    string data;

    /**
     * Minimum terminal string size
     * If the string(or split substrings) size is smaller than this value,
     * we will no longer split it further
     *
     * @note smaller for higher split granularity, but also for more shingle transfer
     */
    size_t terminal_str_size;

    /**
     * The level of the hash shingle tree
     *
     * @warning should be bigger or equal to 2
     * @note smaller for less split operations, but also for higher terminal string transfer(full sync)
     */
    size_t level_num;

    /**
     * The divisor for the actual window size. For a smaller window size
     *
     * @note higher for higher split granularity, but also for creating more shingles and increasing split operations
     */
    size_t partition_num;

    /**
     * The divisor for each layer to gradually decrease the shingle_size(the size of substring for hash).
     *
     * @note higher for higher split granularity, but also for creating more shingles and increasing split operations
     */
    size_t shingle_sz_divisor;

    /**
     * The divisor for each layer to gradually decrease the hash space size for local_min algorithm.
     */
    size_t space_sz_divisor;

    // TODO: to be replaced by SyncMethod passed by users
    GenSync::SyncProtocol base_sync_protocol;

    // Collector of all tree shingles
    vector<shared_ptr<DataObject>> tree_shingles;

    // hash shingle tree
    vector<std::set<shingle>> hash_shingle_tree;

    // lookup dictionary
    // hash -> (string, (start_idx, len))
    unordered_map<size_t, pair<string, pair<size_t, size_t>>> dictionary;

    // The hash value -> the hash values of the split substrings of the string corresponding to the hash value
    // if there are hash collision, it's a kind of error.
    unordered_map<size_t, vector<size_t>> hash_to_substr_hashes;

    // client concerned terminal strings and server sent terminal strings
    unordered_map<size_t, string> terminal_concern, terminal_query;

    // client concerned cycles and server sent cycles
    // should not use hashmap
    map<size_t, cycle> cycle_concern, cycle_query;

private:
    /**
     * retrieve string from bottom(second to last level) to top based on cycle_query
     * (i.e., only backtracking the cycles)
     * @warning should be called after synchronization!
     * @return the recovered string
     */
    string retrieve_string();

    /**
     * Create content dependent partitions based on the input string(hash)
     * And update hash_to_substr_hashes for backtracking
     * @param str_hash the hash of the string
     * @param space smaller-more partitions
     * @param shingle_size inter-relation of the string
     * @return vector of substring hashes in original string order
     */
    vector<size_t> create_hash_vec(size_t str_hash, size_t space = 0, size_t shingle_size = 0);

    /**
     * Update each level of the hash shingles tree based on upper level
     */
    void init_tree_by_string_data();

    /**
     * Generate cycle(non-terminal) and terminal queries, method for client
     * @param otherMinusSelf the shingles that I don't have but other have
     */
    void gen_queries(const list<shared_ptr<DataObject>> &otherMinusSelf);

    /**
     * Answer client's queries, method for server.
     * If it's a shingle of last-level, find the corresponding substring
     * If it's a shingle of other levels, backtracking the lower layer (head is the first hash value of the substring).
     *      Mainly for getting the time we should backtrack
     * @param queries hash values that client do not have
     * @return
     */
    bool answer_queries(std::unordered_set<size_t> &queries);

    /**
     * Insert shingles to a specific level based on the hash_vector
     * @param hash_vector hash vector that stores hash values
     * @param level shingle tree level
     */
    void update_tree_shingles_by_level(const vector<size_t>& hash_vector, uint16_t level);

    /**
     * Find all possible next shingles
     * @param point point(hash value) of one de-Bruijn digraph
     * @param cur current choice
     * @param org original choice
     * @return all possible next shingles
     */
    static vector<shingle> potential_next_shingles(size_t point,
                                                   const map<size_t, vector<shingle>> &cur,
                                                   const map<size_t, vector<shingle>> &org);

    /**
     * Shingle_hashes back to a vector of hashes in the string order
     * For updating cycle's backtracking time(using hashes_vec and updating cyc_info.backtracking_time)
     *      or getting hashes_vec of this string(using cyc_info.backtracking_time and updating hashes_vec)
     * With communicating cyc_info.backtracking_time, we can uniquely get the corresponding string
     * @param cyc_info cycle struct
     * @param tree_level the level of the shingle tree
     * @param hashes_vec a hash train in string order
     * @return if success
     */
    bool shingles_to_substr_hashes(cycle &cyc_info, int level, vector<size_t> &hashes_vec) const;

    // 树 转化为 shingle.first->shingles, 并对shingles排序
    std::map<size_t, vector<shingle>> tree_level_to_shingle_dict(int level) const;

//    // 返回原来的shingle, current_edge更新为shingle的second
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
    string get_str_from_dict_by_hash(size_t hash) {
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

    bool sync_set_pointers_client(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                                  list<shared_ptr<DataObject>> &selfMinusOther, list<shared_ptr<DataObject>> &otherMinusSelf);

    bool sync_set_pointers_server(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                                  list<shared_ptr<DataObject>> &selfMinusOther, list<shared_ptr<DataObject>> &otherMinusSelf);

    /**
     * @param to_del elements to delete
     * @param destroy Destructively delete all delList
     * @return
     */
    bool del_elements_from_set_pointers(const list<shared_ptr<DataObject>> &to_del);

    static vector<size_t> get_local_mins(const vector<size_t> &hash_val, size_t win_size);
};


class RCDS : public SyncMethod {
public:
    // NOT_SET改为0
    // 后三个参数没用(都是在SetOfContent中的参数, RCDS中没用着), 待删除
    RCDS(GenSync::SyncProtocol RCDS_base_proto, shared_ptr<Communicant>& newComm, size_t terminal_str_size = 10, size_t levels = 0,
         size_t partition = 0);

    ~RCDS() = default;

    void addStr(shared_ptr<DataObject>& str); // add folder or file location

    bool SyncClient(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                    list<shared_ptr<DataObject>> &otherMinusSelf) override;

    bool SyncServer(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                    list<shared_ptr<DataObject>> &otherMinusSelf) override;

    string getName() override { return "RCDS Sync."; }

    // 注意是加入文件名的addElem
    bool addElem(shared_ptr<DataObject> newDatum) override;

private:
    GenSync::SyncProtocol m_RCDS_base_proto;
//    size_t m_termStrSize, m_levels, m_partition;
    string m_folder_name;
    vector<shared_ptr<DataObject>> m_filenames;
    map<size_t, string> m_hash2filename;
    bool m_save_file, m_single_file_mode;
//    shared_ptr<Communicant> newCommunicant; // comm for baseSyncProtocol


    // todo: 决定暂时只开放一种
    void set_base_proto(shared_ptr<SyncMethod> &setHost, long mbar, size_t elem_size) {
//        if (GenSync::SyncProtocol::InteractiveCPISync == baseSyncProtocol)
//            setHost = make_shared<InterCPISync>(5, elem_size * 8, 64, 3, true);
//        else if (GenSync::SyncProtocol::CPISync == baseSyncProtocol)
////        else
//            setHost = make_shared<ProbCPISync>(mbar, elem_size * 8, 64, true);
//        else
            setHost = make_shared<InterCPISync>(5, elem_size * 8, 64, 3, true);
//        else if (GenSync::SyncProtocol::InteractiveCPISync == baseSyncProtocol)
//            setHost = make_shared<InterCPISync>(5, elem_size * 8, 64, 3, true);
    }

    bool get_diff_files(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                        vector<shared_ptr<DataObject>> &full_set, list<shared_ptr<DataObject>> &selfMinusOther,
                        list<shared_ptr<DataObject>> &otherMinusSelf) {
        selfMinusOther.clear();
        otherMinusSelf.clear();

        shared_ptr<SyncMethod> setHost;
        SyncMethod::SyncClient(commSync, selfMinusOther, otherMinusSelf);
        set_base_proto(setHost, mbar, elem_size);
        for (auto &dop : full_set) {
            bool ret = setHost->addElem(dop); // Add to GenSync
            Logger::gLog(Logger::METHOD, to_string(ret));
        }

        Logger::gLog(Logger::METHOD, string("Diff: ") + to_string(full_set.size()));

        return setHost->SyncClient(commSync, selfMinusOther, otherMinusSelf);
    };

    bool send_diff_files(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                         vector<shared_ptr<DataObject>> &full_set, list<shared_ptr<DataObject>> &selfMinusOther,
                         list<shared_ptr<DataObject>> &otherMinusSelf) {
        selfMinusOther.clear();
        otherMinusSelf.clear();

        shared_ptr<SyncMethod> setHost;
        SyncMethod::SyncServer(commSync, selfMinusOther, otherMinusSelf);
        set_base_proto(setHost, mbar, elem_size);
        for (auto &dop : full_set) {
            bool ret = setHost->addElem(dop); // Add to GenSync
            Logger::gLog(Logger::METHOD, to_string(ret));
        }

        Logger::gLog(Logger::METHOD, string("Diff: ") + to_string(full_set.size()));

        return setHost->SyncServer(commSync, selfMinusOther, otherMinusSelf);
    };

    /**
     * We check name and file size, hash collision and duplicated filename are strictly prohibited.
     */
    vector<shared_ptr<DataObject>> check_and_get(vector<shared_ptr<DataObject>> &filename_set) {
        vector<shared_ptr<DataObject>> res;
        for (auto &f_name: filename_set) {
            string heuristic_info = f_name->to_string() + to_string(getFileSize(m_folder_name + f_name->to_string()));
            size_t digest = std::hash<std::string>()(heuristic_info);
            if (!m_hash2filename.emplace(digest, f_name->to_string()).second)
                Logger::error_and_quit("Duplicated File name or Hash Collision");
            res.push_back(make_shared<DataObject>(to_ZZ(digest)));
        }
        return res;
    }

    string hash2filename(const ZZ &zz) {
        return m_hash2filename[conv<size_t>(zz)];
    };

    bool string_server(const shared_ptr<Communicant> &commSync, const string& filename, int level, int partition);

    string string_client(const shared_ptr<Communicant> &commSync, const string& filename, int level, int partition);

};

#endif //CPISYNC_RCDS_H
