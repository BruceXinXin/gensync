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
        // use_existing_connection should be true!
        use_existing_connection = true;
        shingle_sz_factor = 2;
        space_sz_factor = 8;
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
    bool recover_str(string& recovered_str);

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
    size_t shingle_sz_factor;

    /**
     * The divisor for each layer to gradually decrease the hash space size for local_min algorithm.
     */
    size_t space_sz_factor;

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
    // only for sever
    unordered_map<size_t, vector<size_t>> hash_to_substr_hashes;

    // client concerned terminal strings and server sent terminal strings
    unordered_map<size_t, string> terminal_concern, terminal_query;

    // client concerned cycles and server sent cycles
    // should not use hashmap, because we need to keep the same order
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
    bool backtracking(cycle &cyc_info, int level, vector<size_t> &hashes_vec) const;

    // TODO: cache the results
    /**
     * Converting shingles in one level into dict, i.e., start point -> shingles.
     * And sort the shingles based on the ending point's value.
     * @param level tree level
     * @return map that contains mapping with start point to shingles
     */
    std::map<size_t, vector<shingle>> tree_level_to_shingle_dict(int level) const;

    /**
     * Sync basic parameters, method for client.
     * @param commSync communicant
     */
    void send_sync_param(const shared_ptr<Communicant> &commSync) const;

    /**
     * Sync basic parameters, method for client.
     * @param commSync communicant
     */
    void recv_sync_param(const shared_ptr<Communicant> &commSync) const;

    // TODO: remove it, let user initialize SyncMethod
    /**
     * Initialize the base SyncMethod
     * @param setHost SyncMethod waiting for being initialized
     * @param mbar (to be deleted)
     * @param elem_size (to be deleted)
     */
    void configure(shared_ptr<SyncMethod> &setHost, long mbar, size_t elem_size);

    /**
     * Insert string into dictionary
     * @param str string to insert
     * @return substring hash
     */
    size_t add_str_to_dict(const string &str) {
        size_t hash = std::hash<string>()(str);
        dictionary.emplace(hash, make_pair(str, make_pair(0, 0)));
        return hash;
    };

    /**
     * Insert substring based on index and length
     * @param idx start index of the substring
     * @param len length of the substring
     * @return substring hash
     */
    size_t add_str_to_dict_by_idx_len(size_t idx, size_t len) {
        size_t hash = std::hash<string>()(data.substr(idx, len));
        dictionary.emplace(hash, make_pair("", make_pair(idx, len)));
        return hash;
    };

    /**
     * Get substring based on its hash
     * @param hash substring hash
     * @return the corresponding string. If the string is not here, return empty string.
     */
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

    /**
     * Get index and length based on substring hash
     * @param hash substring hash
     * @return the corresponding value. If the string is not here, return {0, 0}.
     */
    pair<size_t, size_t> get_idx_len_by_hash(size_t hash) {
        auto it = dictionary.find(hash);
        if (it != dictionary.end())
            return it->second.second;

        return {0, 0};
    }

    /**
     * Return multiple shingles' hash values
     * @param set multiple shingles
     * @return shingles' unique hash values.
     */
    static vector<size_t> get_unique_hashes_from_shingles(const std::set<shingle>& set) {
        std::set<size_t> tmp;
        for (const shingle& item : set) {
            tmp.insert(item.first);
            tmp.insert(item.second);
        }
        return vector<size_t>(tmp.begin(), tmp.end());
    }

    /**
     * Get unique shingles from hash shingle tree.
     */
    vector<ZZ> get_unique_shingleZZs_from_tree() {
        std::set<ZZ> res;
        for (const auto& tree_level: hash_shingle_tree) {
            for (const auto& item: tree_level)
                res.emplace(TtoZZ(item));
        }
        return { res.begin(), res.end() };
    };

    /**
     * Sync tree shingle, method for client
     * @param commSync communicant
     * @param mbar (to be deleted)
     * @param elem_size  (to be deleted)
     * @param selfMinusOther elements that I have while other do not
     * @param otherMinusSelf elements that other haves while I do not
     * @return if success
     */
    bool sync_tree_shingles_client(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                                   list<shared_ptr<DataObject>> &selfMinusOther, list<shared_ptr<DataObject>> &otherMinusSelf);

    /**
     * Sync tree shingle, method for server
     * @param commSync communicant
     * @param mbar (to be deleted)
     * @param elem_size  (to be deleted)
     * @param selfMinusOther elements that I have while other do not
     * @param otherMinusSelf elements that other haves while I do not
     * @return if success
     */
    bool sync_tree_shingles_server(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                                   list<shared_ptr<DataObject>> &selfMinusOther, list<shared_ptr<DataObject>> &otherMinusSelf);

    /**
     * @param to_del elements to delete
     * @return if we delete all the elements in to_del
     */
    bool del_elements_from_set_pointers(const list<shared_ptr<DataObject>> &to_del);

    /**
     * Calculate local minimum indexes based on window size
     * @param hash_val the hash value to be calculated
     * @param win_size window size
     * @return The indexes of the local minimum hashes.
     */
    static vector<size_t> get_local_mins(const vector<size_t> &hash_val, size_t win_size);
};


class RCDS : public SyncMethod {
public:
    RCDS(GenSync::SyncProtocol RCDS_base_proto, size_t terminal_str_size = 10, size_t levels = 0,
         size_t partition = 0);

    ~RCDS() override = default;

    /**
     * See register_file
     * @param newDatum folder name or file name
     * @return if success
     */
    bool addElem(shared_ptr<DataObject> newDatum) override;

    bool SyncClient(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                    list<shared_ptr<DataObject>> &otherMinusSelf) override;

    bool SyncServer(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                    list<shared_ptr<DataObject>> &otherMinusSelf) override;

    string getName() override { return "RCDS Sync."; }

private:
    // TODO: change it to SyncMethod
    GenSync::SyncProtocol m_RCDS_base_proto;

    // see RCDS_Synchronizer
    size_t m_terminal_str_size, m_levels, m_partition;

    // folder name or file name to be synchronized
    string m_target;

    // if m_target is a folder, we store filenames in it.
    vector<shared_ptr<DataObject>> m_filenames;

    // for quick lookup
    map<size_t, string> m_hash_to_filename;

    // if save file after synchronization
    bool m_save_file;

    // if m_target is a filename
    bool m_single_file_mode;

private:
    /**
     * Add files in folder or just add a single file
     * @param str folder name or file name
     */
    void register_file(shared_ptr<DataObject>& str);

    // TODO: remove it, replaced by SyncMethod passed by user
    void set_base_proto(shared_ptr<SyncMethod> &setHost, long mbar, size_t elem_size);

    /**
     * Get file(s) that is(are) different, method for client
     * @param commSync communicant
     * @param mbar (to be deleted)
     * @param elem_size (to be deleted)
     * @param full_set filename(s)
     * @param selfMinusOther different file's filename(s)
     * @param otherMinusSelf not useful, but we should keep it
     * @return if sync success
     */
    bool get_diff_files(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                          vector<shared_ptr<DataObject>> &full_set, list<shared_ptr<DataObject>> &selfMinusOther,
                          list<shared_ptr<DataObject>> &otherMinusSelf);

    /**
     * Get file(s) that is(are) different, method for server
     * @param commSync communicant
     * @param mbar (to be deleted)
     * @param elem_size (to be deleted)
     * @param full_set filename(s)
     * @param selfMinusOther different file's filename(s)
     * @param otherMinusSelf not useful, but we should keep it
     * @return if sync success
     */
    bool send_diff_files(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                         vector<shared_ptr<DataObject>> &full_set, list<shared_ptr<DataObject>> &selfMinusOther,
                         list<shared_ptr<DataObject>> &otherMinusSelf);

    /**
     * Sync string, method for server
     * @param commSync communicant
     * @param filename name of file that needed to be synced
     * @param level see RCDS_Synchronizer
     * @param partition see RCDS_Synchronizer
     * @return if success
     */
    bool string_server(const shared_ptr<Communicant> &commSync, const string& filename, int level, int partition);

    /**
     * Sync string, method for client
     * @param commSync communicant
     * @param filename name of file that needed to be synced
     * @param level see RCDS_Synchronizer
     * @param partition see RCDS_Synchronizer
     * @return reconstructed string
     */
    string string_client(const shared_ptr<Communicant> &commSync, const string& filename, int level, int partition);

    /**
     * Put filenames into mapping dict, and check if there are hash collision and duplicated filename.
     * @param filename_set filename(in DataObject) set
     * @return hash(es) (in DataObject) of filenames
     */
    vector<shared_ptr<DataObject>> check_and_get(vector<shared_ptr<DataObject>> &filename_set) {
        vector<shared_ptr<DataObject>> res;
        for (const auto &f_name: filename_set) {
            string heuristic_info = f_name->to_string() + to_string(getFileSize(m_target + f_name->to_string()));
            size_t digest = std::hash<std::string>()(heuristic_info);
            if (!m_hash_to_filename.emplace(digest, f_name->to_string()).second)
                Logger::error_and_quit("Duplicated File name or Hash Collision");
            res.push_back(make_shared<DataObject>(to_ZZ(digest)));
        }
        return res;
    }
};

#endif //CPISYNC_RCDS_H
