//
// Created by bruce on 2/16/2023.
//

#include <cassert>
#include <unordered_map>

#include <CPISync/Syncs/RCDS.h>

//#define RCDS_RECORD_OPERATIONS_TIME

//RCDS_Synchronizer::RCDS_Synchronizer(size_t terminal_str_size, size_t levels, size_t partition,
//                                     GenSync::SyncProtocol base_set_proto, size_t shingle_size, size_t space)
//        : terminal_str_size(terminal_str_size), level_num(levels), partition_num(partition), base_sync_protocol(base_set_proto),
//          shingle_sz(shingle_size), space_sz(space) {
//    if (levels < 2 || USHRT_MAX < levels)
//        Logger::error_and_quit("levels should be in the range of [2, uint16 max]");
//    use_existing_connection = false;
//}

bool RCDS_Synchronizer::add_str(const string &str) {
    string tmp = str;
    return add_str(move(tmp));
}

bool RCDS_Synchronizer::add_str(string&& str) {
    data = move(str);

    if (data.empty()) return false;

    if (level_num <= 1)
        Logger::error_and_quit("Level cannot <= 1!");

    init_tree_by_string_data();

    tree_shingles.clear();
    for (const auto& item : get_unique_shingleZZs_from_tree())
        tree_shingles.emplace_back(new DataObject(item));
    return true;
}

bool RCDS_Synchronizer::SyncServer(const shared_ptr<Communicant> &commSync) {
    Logger::gLog(Logger::METHOD, "Entering RCDS_Synchronizer::SyncServer");
    if (!use_existing_connection) {
        Logger::gLog(Logger::METHOD, "Chose not use existing connection.");
        commSync->commListen();
        recv_sync_param(commSync);
        Logger::gLog(Logger::METHOD, "Sync params success!");
    }

    long mbar = 0;
    if (GenSync::SyncProtocol::CPISync == base_sync_protocol) { // Not well supported
        mbar = 1 << 10;
    }

    bool success = false;
    {
        size_t top_mbar = pow(2 * partition_num, level_num) * 2; // Upper bound on the number of symmetrical difference
        // If failed, try bigger mbar, inspired by Bowen
        list<shared_ptr<DataObject>> selfMinusOther, otherMinusSelf;
        while (!success && mbar < top_mbar) {
            success = sync_tree_shingles_server(commSync, mbar, sizeof(shingle), selfMinusOther, otherMinusSelf);
            success &= (SYNC_SUCCESS == commSync->commRecv_int());
            commSync->commSend(success? SYNC_SUCCESS: SYNC_FAILURE);
            if (success)
                break;

            Logger::gLog(Logger::METHOD,
                         "RCDS_Synchronizer::SyncServer - mbar doubled from " + to_string(mbar) + " to " +
                         to_string(2 * (mbar + 1)));
            mbar = 2 * (mbar + 1);
        }
    }

    // get query size
    size_t query_size = commSync->commRecv_size_t();
    {
        std::unordered_set<size_t> queries;
        // get queries
        for (size_t i = 0; i < query_size; ++ i) {
            size_t res = commSync->commRecv_size_t();
            queries.emplace(res);
        }

        // should keep order
        if (!answer_queries(queries))
            cerr << "Failed to answer all the queries, this sync should fail!" << endl;
    }

    Logger::gLog(Logger::METHOD,
                 "Answered " + to_string(cycle_concern.size()) + " cycles and " +
                 to_string(terminal_concern.size()) + " hashes.");

    for (const auto& dic : terminal_concern) {
        string tmp_str = get_str_from_dict_by_hash(dic.first);
        commSync->commSend(tmp_str.empty()? "E": tmp_str);
    }

    for (const auto& cyc : cycle_concern)
        commSync->commSend(TtoZZ(cyc.second), sizeof(cycle));

    Logger::gLog(Logger::METHOD, "Server RCDS Sync part finishes!");
    if (!use_existing_connection)
        commSync->commClose();

    return success;
}

bool RCDS_Synchronizer::SyncClient(const shared_ptr<Communicant> &commSync) {

    Logger::gLog(Logger::METHOD, "Entering RCDS_Synchronizer::SyncClient");
    if (!use_existing_connection) {
        Logger::gLog(Logger::METHOD, "Chose not use existing connection.");
        commSync->commConnect();
        send_sync_param(commSync);
        Logger::gLog(Logger::METHOD, "Sync params success!");
    }

    long mbar = 0;
    if (GenSync::SyncProtocol::CPISync == base_sync_protocol) {
        mbar = 1 << 10;
    }

#ifdef RCDS_RECORD_OPERATIONS_TIME
    auto start_pq = std::chrono::high_resolution_clock::now();
#endif
    bool success = false;
    list<shared_ptr<DataObject>> selfMinusOther, otherMinusSelf;
    size_t top_mbar = pow(2 * partition_num, level_num) * 2; // Upper bound on the number of symmetrical difference
    while (!success and mbar < top_mbar) { // if set recon failed, This can be caused by error rate and small mbar
        success = sync_tree_shingles_client(commSync, mbar, sizeof(shingle), selfMinusOther, otherMinusSelf);
        commSync->commSend(success? SYNC_SUCCESS: SYNC_FAILURE);
        success &= (SYNC_SUCCESS == commSync->commRecv_int());
        if (success)
            break;

        Logger::gLog(Logger::METHOD,
                     "RCDS_Synchronizer::SyncClient - mbar doubled from " + to_string(mbar) + " to " +
                     to_string(2 * (mbar + 1)));
        mbar = 2 * (mbar + 1);
    }
#ifdef RCDS_RECORD_OPERATIONS_TIME
    auto end_pq = std::chrono::high_resolution_clock::now();
    cout << std::chrono::duration_cast<std::chrono::microseconds>(end_pq - start_pq).count() << " ";
#endif

    // before the query
    gen_queries(otherMinusSelf);

    // send queries
    commSync->commSend(cycle_query.size() + terminal_query.size());
    // ask about cycles
    for (const auto& p: cycle_query)
        commSync->commSend(p.first);
    // ask about terms
    for (const auto& p: terminal_query)
        commSync->commSend(p.first);

    // add to dict
    for (int i = 0; i < terminal_query.size(); ++ i) {
        auto tmp = commSync->commRecv_string();
        if (tmp != "E") {
            // non-empty string
            cycle_query.erase(add_str_to_dict(tmp));
        }
    }

    for (auto &cyc: cycle_query)
        cyc.second = ZZtoT<cycle>(commSync->commRecv_ZZ(sizeof(cycle)));

    Logger::gLog(Logger::METHOD, "Client RCDS Sync part finishes!");
    if (!use_existing_connection)
        commSync->commClose();

    // add oms, remove smo(we dont need to handle elements in server)
    tree_shingles.reserve(tree_shingles.size() + otherMinusSelf.size());
    for (auto& oms: otherMinusSelf)
        tree_shingles.emplace_back(move(oms));
    del_elements_from_set_pointers(selfMinusOther);

    return success;
}

bool RCDS_Synchronizer::recover_str(string& recovered_str) {
    hash_shingle_tree.clear();
    hash_shingle_tree.resize(level_num);
    for (const auto& s_zz : tree_shingles) {
        shingle s = ZZtoT<shingle>(s_zz->to_ZZ());
        hash_shingle_tree[s.level].insert(s);
    }

    data = retrieve_string();
    // maybe we cannot simply move it
    recovered_str = data;
    return true;
}

string RCDS_Synchronizer::retrieve_string() {
//    cout << "C queries size: " << cycle_query.size() << endl;

    // retrieve string from bottom
    string substring;
    for (int i = hash_shingle_tree.size() - 2; i >= 0; -- i) {
        for (const auto& shingle : hash_shingle_tree[i]) {
            auto it = cycle_query.find(shingle.second);
            if (it != cycle_query.end()) {
                vector<size_t> temp;
                substring.clear();
                auto& tmp_cycle = it->second;

//                cout << tmp_cycle.backtracking_time << endl;
//                if (std::hash<string>()(dictionary[tmp_cycle.head].first) != tmp_cycle.head){
//                    cout << (dictionary.find(tmp_cycle.head) == dictionary.end()) << endl;
//                    cout << (tmp_cycle.head == 12573527631396608276ul) << endl;
//                    cout << std::hash<string>()(dictionary[tmp_cycle.head].first) << " " << tmp_cycle.head << endl;
//                    cout << std::hash<string>()(dictionary[12573527631396608276ul].first) << " " << tmp_cycle.head << endl;
//                    cout << dictionary[tmp_cycle.head].first.size() << endl;
//                }

//                size_t sz = dictionary[12573527631396608276ul].first.size();
//                cout << sz << endl;
//                cout << hash<string>()(substring) << endl;
//                size_t h = hash<string>()(dictionary[12573527631396608276ul].first);
//                cout << h << endl;

//                cout << tmp_cycle.backtracking_time << endl;
                if (!backtracking2(tmp_cycle, i + 1, temp)) {
                    substring = get_str_from_dict_by_hash(shingle.second);
                    cerr << "client backtracking() return false." << endl;
                }

//                sz = dictionary[12573527631396608276ul].first.size();
//                cout << sz << endl;
//                cout << hash<string>()(substring) << endl;
//                cout << hash<string>()(dictionary[12573527631396608276ul].first) << endl;

//                // for debug
//                cout << "C:";
//                for (size_t t: temp)
////                    cout << (!dictionary[t].first.empty()? dictionary[t].first.size(): -1) << ",";
//                    cout << t << ",";
//                cout << endl;

                for (size_t hash: temp) {
                    if (dictionary.find(hash) == dictionary.end())
                        // TODO: exit?
                        cerr << "Recover may have failed - Dictionary lookup failed for " << hash << " at level "
                             << shingle.level << endl;
                    substring += get_str_from_dict_by_hash(hash);
                }

//                sz = dictionary[12573527631396608276ul].first.size();
//                cout << sz << endl;
//                cout << hash<string>()(substring) << endl;
                add_str_to_dict(substring);
//                sz = dictionary[12573527631396608276ul].first.size();
//                cout << sz << endl;
//                cout << hash<string>()(substring) << endl;
           }
        }
    }

    return substring.empty()? data: substring;
}

vector<size_t> RCDS_Synchronizer::create_hash_vec(size_t str_hash, size_t space, size_t shingle_size) {
    vector<size_t> hash_val, hash_set;
    auto p = get_idx_len_by_hash(str_hash);
    if (p.second == 0)
        return hash_set;
    auto str = get_str_from_dict_by_hash(str_hash);
    size_t win_size = floor((p.second / partition_num) / 2);

    // substring size should not smaller than terminal string size
    if (p.second <= terminal_str_size) {
        hash_set = {str_hash};
    } else {
        if (space == 0)
            Logger::error_and_quit("Space cannot be 0!");
        if (shingle_size < 2)
            Logger::error_and_quit("Shingle size should < 2!");

        // get hash values
        hash_val.reserve(str.size() - shingle_size + 1);
        for (size_t i = 0; i < str.size() - shingle_size + 1; ++i) {
            static std::hash<std::string> hasher;
            hash_val.push_back(hasher(str.substr(i, shingle_size)) % space);
        }

        auto local_mins = get_local_mins(hash_val, win_size);
        hash_set.reserve(local_mins.size() + 1);
        size_t prev = p.first;
        // add these split substrings to hashset
        for (size_t min: local_mins) {
            min += p.first;
            hash_set.push_back(add_str_to_dict_by_idx_len(prev, min - prev));
            prev = min;
        }
        hash_set.push_back(add_str_to_dict_by_idx_len(prev, p.second - (prev - p.first)));
    }

    // write it to cyc-dict
    auto cyc_it = hash_to_substr_hashes.find(str_hash);
    // simply updates
    if (cyc_it == hash_to_substr_hashes.end())
        hash_to_substr_hashes[str_hash] = hash_set;
    else if (cyc_it->second != hash_set) {
        // this indicates that we didn't split this string because the size is smaller than terminal size
        if (cyc_it->second.size() == 1 && cyc_it->second.front() == cyc_it->first)
            hash_to_substr_hashes[str_hash] = hash_set;
        // hash collision
        else
            Logger::error_and_quit("More than one answer is possible for hash_to_substr_hashes!");
    }

    return hash_set;
}

void RCDS_Synchronizer::init_tree_by_string_data() {
    hash_shingle_tree.clear(); // should be redundant

    double supposed_str_len = pow(partition_num, level_num) * terminal_str_size; // supposed string size

    if (supposed_str_len < 1)
        Logger::error_and_quit(
                "Error params! level_num: " + to_string(level_num) + ", terminalStrSz: "
                + to_string(terminal_str_size) + ", actual string size: " + to_string(data.size()));

    size_t shingle_size = 2 * pow(shingle_sz_factor, level_num); // size of window
    if (shingle_size < 1)
        Logger::error_and_quit("Shingle size cannot < 1!");
    size_t space = 4 * pow(space_sz_factor, level_num); // hash space
    if (space < 1)
        Logger::error_and_quit("Hash space cannot < 1!");

    vector<size_t> cur_level_hashes;
    hash_shingle_tree.resize(level_num);

    // level 0
    update_tree_shingles_by_level({add_str_to_dict_by_idx_len(0, data.size())}, 0);

    // level [1, level_num)
    for (int level = 1; level < level_num; ++ level) {
        // get hash vec from upper level and update the current level
        for (auto substr_hash: get_unique_hashes_from_shingles(hash_shingle_tree[level - 1])) {
            cur_level_hashes = create_hash_vec(substr_hash, space, shingle_size);
            update_tree_shingles_by_level(cur_level_hashes, level);
        }
        space = floor(space / space_sz_factor);
        shingle_size = floor(shingle_size / shingle_sz_factor);
    }
}

void RCDS_Synchronizer::gen_queries(const list<shared_ptr<DataObject>> &otherMinusSelf) {
    cycle_query.clear();
    terminal_query.clear();

    std::unordered_set<size_t> tmp;
    for (const auto& shingle_zz: otherMinusSelf) {
        auto s = ZZtoT<shingle>(shingle_zz->to_ZZ());
        // lower level is preferred for less backtracking
        if (tmp.emplace(s.second).second)
            cycle_query.erase(s.second);

        if (dictionary.find(s.second) == dictionary.end()) {
            // shingle in non-terminal level
            if (s.level < level_num - 1)
                cycle_query.emplace(s.second, cycle{0, 0, 0});
            else
                terminal_query.emplace(s.second, "");
        }
    }
}

bool RCDS_Synchronizer::answer_queries(std::unordered_set<size_t> &queries) {
    cycle_concern.clear();
    terminal_concern.clear();

//    cout << "S queries size: " << queries.size() << endl;

    // from bottom to top
    for (auto rit = hash_shingle_tree.rbegin(); rit != hash_shingle_tree.rend(); ++ rit) {
        for (const auto& shingle : *rit) {
            auto it = queries.find(shingle.second);
            if (it != queries.end()) {
                // shingle in terminal level, simply send terminal string afterward
                if (level_num - 1 == shingle.level)
                    terminal_concern.emplace(shingle.second, get_str_from_dict_by_hash(shingle.second));
                else {
                    // server backtracking
                    auto& tmp_vec = hash_to_substr_hashes[shingle.second];

//                    // for debug
//                    cout << "S:";
//                    for (size_t t: tmp_vec)
////                        cout << dictionary[t].second.second << ",";
//                        cout << t << ",";
//                    cout << endl;

                    cycle tmp = cycle{.head = tmp_vec.front(), .len = static_cast<uint32_t>(tmp_vec.size()), .backtracking_time=0};

                    if (backtracking2(tmp, shingle.level + 1, tmp_vec)) {
                        cycle_concern[shingle.second] = tmp;
                    }
                    else {
                        cerr << "server backtracking failed!" << endl;
                        continue; // failed
                    }
                }

                // if success, erase this query
                queries.erase(it);
            }

            // no need to continuously iterate
            if (queries.empty())
                return true;
        }
    }
    return queries.empty();
}

void RCDS_Synchronizer::update_tree_shingles_by_level(const vector<size_t>& hash_vector, uint16_t level) {
    if (hash_shingle_tree.size() <= level)
        Logger::error_and_quit("hash_shingle_tree assert failed! Its size cannot <= level!");
    if (hash_vector.empty())
        return;
    if (hash_vector.size() > 100)
        cout << "Hash partitions should not exceed 100, otherwise the performance will be bad." << endl;

    map<pair<size_t, size_t>, size_t> tmp;

    // shingle head node: [i - 1], shingle end node: [i]
    for (int i = 0; i < hash_vector.size(); ++ i) {
        size_t first = 0;
        if (i > 0)
            first = hash_vector[i - 1];

        auto it = tmp.find({first, hash_vector[i]});
        if (it != tmp.end())
            ++ it->second;
        else
            // first, second
            tmp[{first, hash_vector[i]}] = 1;
    }
    if (tmp.empty())
        Logger::error_and_quit("Shingle results cannot be empty!");

    for (const auto& item : tmp) {
        if (item.second > USHRT_MAX)
            Logger::error_and_quit("Shingle occurance overflows (> u16_MAX).");
        hash_shingle_tree[level].insert(shingle{item.first.first, item.first.second, level, (uint16_t) item.second});
    }
}

vector<shingle> RCDS_Synchronizer::potential_next_shingles(size_t point,
                                                           const map<size_t, vector<shingle>> &cur,
                                                           const map<size_t, vector<shingle>> &org) {
    vector<shingle> res_vec;

    auto curr = cur.find(point);

    // give priority to cur
    if (curr != cur.end()) {
        for (const auto& tmp_shingle: curr->second)
            if (tmp_shingle.occur_time > 0)
                res_vec.push_back(tmp_shingle);
    }
    else {
        auto cur_it = org.find(point);
        // stop lookup if org is empty
        if (cur_it == org.end())
            return res_vec;

        for (const auto& tmp_shingle: cur_it->second)
            if (tmp_shingle.occur_time > 0)
                res_vec.push_back(tmp_shingle);
    }

    return res_vec;
}

bool RCDS_Synchronizer::backtracking(cycle &cyc_info, int level, vector<size_t>& hashes_vec)  {
    if (hash_shingle_tree[level].empty())
        Logger::error_and_quit("tree_level cannot be empty!");

    // get shingle.first->shingles from this tree level
    map<size_t, vector<shingle>> original_state = tree_level_to_shingle_dict(level);

    vector<map<size_t, vector<shingle>>> cur_state;
    // push one dummy object
    cur_state.emplace_back();

    vector<vector<shingle>> next_edges_stk;
    size_t dfs_time = 0;
    size_t cur_point = 0;
    vector<size_t> res;

    // start from 0(head)
    for (const auto& head_shingles : original_state[0]) {
        if (cyc_info.head == head_shingles.second) {
            res.push_back(head_shingles.second);
            cur_point = head_shingles.second;
            break;
        }
    }

    // find head from "hashes_vec"
    // start from 0
    if (cyc_info.backtracking_time == 0) {
        // just one element, return
        if (hashes_vec.size() == 1 && cyc_info.len == 1) {
            cyc_info.backtracking_time = 1;
            hashes_vec = res;
            return true;
        }
    }
    // cycle number is 1
    else if (cyc_info.backtracking_time == 1 && cyc_info.len == 1) {
        hashes_vec = res;
        return true;
    }

    // non-recursive dfs
    shingle last_edge{};
    while (!cur_state.empty() && cur_state.size() == next_edges_stk.size() + 1) { // while state stack is not empty
        vector<shingle> next_shingles = potential_next_shingles(cur_point, cur_state.back(), original_state);

        // If we can go further with this route
        if (!next_shingles.empty() && res.size() < cyc_info.len) {
            last_edge = next_shingles.back();
            cur_point = last_edge.second;
            next_shingles.pop_back();
            next_edges_stk.push_back(next_shingles);
        }
            // if this route is dead, we should look for other options
        else if (!next_edges_stk.empty() && cur_state.size() == next_edges_stk.size() + 1 &&
                 !next_edges_stk.back().empty()) {
            if (!res.empty())
                res.pop_back();

            // look for other edge options
            last_edge = next_edges_stk.back().back();
            cur_point = last_edge.second;
            next_edges_stk.back().pop_back();
            cur_state.pop_back();
        }
            // if this state is dead and we should look back a state
        else if (!cur_state.empty() && cur_state.size() == next_edges_stk.size() + 1 && !next_edges_stk.empty() &&
                 next_edges_stk.back().empty()) {
            if (!res.empty()) res.pop_back();
            // look back a state or multiple if we have empty next choice (unique next edge)
            while (!next_edges_stk.empty() && next_edges_stk.back().empty()) {
                next_edges_stk.pop_back();
                cur_state.pop_back();
                if (!res.empty())
                    res.pop_back();
            }
            if (next_edges_stk.empty()) {
                return false;
            }
            else if (!next_edges_stk.back().empty()) {
                last_edge = next_edges_stk.back().back();
                cur_point = last_edge.second;
                next_edges_stk.back().pop_back();
                cur_state.pop_back();
            }
        }
        else if (cur_state.size() != next_edges_stk.size() + 1) {
            Logger::error_and_quit("cur_state and next_edge_stack size do not match! " + to_string(cur_state.size())
                                   + ":" + to_string(next_edges_stk.size()));
        }

        res.push_back(cur_point);

        // Change and register our state for shingle occurrence and nxt edges
        auto& tmp_stack = cur_state.back();
        bool found = false;
        auto frm_stk = tmp_stack.find(last_edge.first);
        // find the last_edge in from stack first
        if (frm_stk != tmp_stack.end()) {
            for (auto &tmp_shingle: frm_stk->second) {
                if (tmp_shingle == last_edge) {
                    -- tmp_shingle.occur_time;
                    found = true;
                    break;
                }
            }
        }
        // find the last_edge in original state stack if we cannot find it in from stack
        if (!found) {
            for (auto& tmp_shingle: original_state[last_edge.first]) {
                if (tmp_shingle == last_edge)
                    -- tmp_shingle.occur_time;
                tmp_stack[tmp_shingle.first].push_back(tmp_shingle);
            }
        }

        cur_state.push_back(tmp_stack);

        // if we reached a stop point
        if (res.size() == cyc_info.len) {
            ++ dfs_time;
            if (res == hashes_vec || (dfs_time == cyc_info.backtracking_time && cyc_info.backtracking_time != 0)) {
                cyc_info.backtracking_time = dfs_time;
                hashes_vec = move(res);

                // end iteration
                return true;
            }
        }
    }
    return false;
}

bool RCDS_Synchronizer::backtracking2(cycle &cyc, int level, vector<size_t> &hashes_vec) {
    if (hash_shingle_tree[level].empty())
        Logger::error_and_quit("tree_level cannot be empty!");

    // get shingle.first->shingles from this tree level
    // avoid copying data
    auto& hash_to_shingles = tree_level_to_shingle_dict(level);

    vector<size_t> cur;
    // parent, self, which_child_should_we_start_dfs_now
    stack<tuple<size_t, size_t, size_t>> stk;
    // find head in $0's next points
    // because we use binary search, vector<shingle> should be sorted by shingle.second
    {
        auto it = lower_bound(hash_to_shingles[0].begin(), hash_to_shingles[0].end(), cyc.head, [](const shingle& s1, size_t target){
            return s1.second < target;
        });
        assert(it != hash_to_shingles[0].end());
        cur.push_back(it->second);
        stk.emplace(0, it->second, 0);
    }

    size_t removed_edges = 0;
    size_t backtracking_time = 0;
    while (!stk.empty()) {
        size_t parent, v;
        size_t& num = get<2>(stk.top());
        {
            auto& tmp = stk.top();
            parent = get<0>(tmp);
            v = get<1>(tmp);
        }

        bool recover_edge = false;
        // reach an end
        if (cur.size() == cyc.len) {
            ++ backtracking_time;
            // (client backtracking && backtracking time reaches end) || (server backtracking && find the path)
            if ((backtracking_time != 0 && backtracking_time == cyc.backtracking_time) || cur == hashes_vec) {
                cyc.backtracking_time = backtracking_time;
                hashes_vec = move(cur);

                // end iteration
//                cout << removed_edges << " " << hashes_vec.size() << endl;
                assert(removed_edges == hashes_vec.size() - 1);
                return true;
            }

            // this path is not our target path
            recover_edge = true;
        }
        // still not reach to the end
        else {
            bool updated = false;
            auto& vec = hash_to_shingles[v];
            for (size_t i = num; i < vec.size(); ++ i) {
                auto& s = vec[i];
                if (s.occur_time > 0) {
                    -- s.occur_time;
                    ++ removed_edges;
                    num = i + 1;
                    stk.emplace(v, s.second, 0);
                    cur.push_back(s.second);

                    updated = true;
                    break;
                }
            }

            // no more valid edge
            if (!updated)
                recover_edge = true;
        }

        // recover edge's occur_time
        // must implement it when we cache the hash_to_shingles
        // because we use binary search, vector<shingle> should be sorted by shingle.second
        if (recover_edge) {
            // be aware of the order of s1.second and target, debug for quite a long time!
            auto it = lower_bound(hash_to_shingles[parent].begin(), hash_to_shingles[parent].end(), v, [](const shingle& s1, size_t target){
                return s1.second < target;
            });
            assert(it != hash_to_shingles[parent].end());
            ++ it->occur_time;
            -- removed_edges;

            // at the same time, we should remove this node
            cur.pop_back();
            stk.pop();
        }
    }

    return false;
}

std::map<size_t, vector<shingle>>& RCDS_Synchronizer::tree_level_to_shingle_dict(int level) {
//    static vector<std::map<size_t, vector<shingle>>> cache;
    // make sure the cache is initialized
    if (cache.empty())
        cache = vector<std::map<size_t, vector<shingle>>>(hash_shingle_tree.size());

    auto& res = cache[level];
//    std::map<size_t, vector<shingle>> res;
    if (res.empty()) {
        for (const auto& shingle : hash_shingle_tree[level])
            res[shingle.first].push_back(shingle);

        for (auto &shingle: res)
            std::sort(shingle.second.begin(), shingle.second.end());
    }

//    if (level == 1)
//        cache.clear();
    return res;
}


void RCDS_Synchronizer::send_sync_param(const shared_ptr<Communicant> &commSync) const {
    Logger::gLog(Logger::METHOD, "Entering SendSyncParam::send_sync_param");
    commSync->commSend((long) terminal_str_size);
    commSync->commSend((long) level_num);
    commSync->commSend((long) partition_num);
    if (commSync->commRecv_byte() == SYNC_FAIL_FLAG)
        throw SyncFailureException("Sync params do not match!");
}

void RCDS_Synchronizer::recv_sync_param(const shared_ptr<Communicant> &commSync) const {
    Logger::gLog(Logger::METHOD, "Entering RCDS_Synchronizer::recv_sync_param");

    size_t remote_terminal_str_size = commSync->commRecv_long();
    size_t remote_level_num = commSync->commRecv_long();
    size_t remote_partition_num = commSync->commRecv_long();

    if (remote_terminal_str_size != terminal_str_size || remote_level_num != level_num || remote_partition_num != partition_num) {
        // do not match
        commSync->commSend(SYNC_FAIL_FLAG);
        Logger::gLog(Logger::COMM, "Sync params do not match! (Client: Server)"
                                   " (" + to_string(remote_terminal_str_size) + ", " + to_string(terminal_str_size) + ") (" +
                                   to_string(remote_level_num) + "," + to_string(level_num) + ") (" +
                                   to_string(partition_num) + "," + to_string(remote_partition_num) + ").");
        throw SyncFailureException("Sync params do not match!");
    }
    commSync->commSend(SYNC_OK_FLAG);
}

void RCDS_Synchronizer::configure(shared_ptr<SyncMethod> &setHost, long mbar, size_t elem_size) {
//    if (GenSync::SyncProtocol::IBLTSyncSetDiff == base_sync_protocol)
//        setHost = make_shared<IBLTSync_SetDiff>(mbar, elem_size, true);
    if (GenSync::SyncProtocol::InteractiveCPISync == base_sync_protocol)
//        setHost = make_shared<InterCPISync>(5, elem_size * 8, 64, 3, true);
        setHost = make_shared<InterCPISync>(2 * UCHAR_MAX, pow((double) sizeof(randZZ()), 2.0), 8, 5, true);
}

bool RCDS_Synchronizer::sync_tree_shingles_client(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                                                  list<shared_ptr<DataObject>> &selfMinusOther, list<shared_ptr<DataObject>> &otherMinusSelf) {
    selfMinusOther.clear();
    otherMinusSelf.clear();
    shared_ptr<SyncMethod> setHost;

    configure(setHost, mbar, elem_size);
    for (auto& dop : tree_shingles) {
        setHost->addElem(dop);
    }
    bool success = setHost->SyncClient(commSync, selfMinusOther, otherMinusSelf);
    if (!del_elements_from_set_pointers(selfMinusOther))
        Logger::error_and_quit("We failed to delete all elements!");
    for (const auto& item : otherMinusSelf)
        tree_shingles.push_back(item);

    return success;
}

bool RCDS_Synchronizer::sync_tree_shingles_server(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                                                  list<shared_ptr<DataObject>> &selfMinusOther, list<shared_ptr<DataObject>> &otherMinusSelf) {
    selfMinusOther.clear();
    otherMinusSelf.clear();
    shared_ptr<SyncMethod> setHost;

    configure(setHost, mbar, elem_size);
    for (const auto& dop : tree_shingles)
        setHost->addElem(dop);
    return setHost->SyncServer(commSync, selfMinusOther, otherMinusSelf);
}

bool RCDS_Synchronizer::del_elements_from_set_pointers(const list<shared_ptr<DataObject>> &to_del) {
    std::set<ZZ> tmp;
    for (auto& item : to_del)
        tmp.insert(item->to_ZZ());

    auto it = tree_shingles.begin();
    while (it != tree_shingles.end()) {
        auto to_del_it = tmp.find((*it)->to_ZZ());
        if (to_del_it != tmp.end()) {
            it = tree_shingles.erase(it);
            tmp.erase(to_del_it);
        }
        else ++ it;
    }

    return tmp.empty();
}

vector<size_t> RCDS_Synchronizer::get_local_mins(const vector<size_t> &hash_val, size_t win_size) {
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

    // add 2 * win_size + 1 hashes to map first
    // We should use RB tree to maintain order.
    map<size_t, size_t> hash_occur_cnt;
    for (size_t j = 0; j < 2 * win_size + 1; ++ j)
        ++ hash_occur_cnt[hash_val[j]];

    // i is the center of window
    for (size_t i = win_size; i < hash_val.size() - win_size; ++ i) {
        // smaller than smallest hash && bigger than one window
        if (hash_val[i] <= hash_occur_cnt.begin()->first && ((mins.empty())? i : i - mins.back()) > win_size)
            mins.push_back(i);

        // jump out of the loop in the end
        if (i + win_size + 1 == hash_val.size())
            break;

        // if the hashes in window's left and window's right are the same, skip updating
        if (hash_val[i - win_size] == hash_val[i + win_size + 1])
            continue;

        // remove hash in window's left
        auto it_prev = hash_occur_cnt.find(hash_val[i - win_size]);
        if (it_prev != hash_occur_cnt.end()) {
            if (it_prev->second > 1)
                -- it_prev->second;
            else hash_occur_cnt.erase(it_prev);
        }

        // add hash in window's right
        ++ hash_occur_cnt[hash_val[i + win_size + 1]];
    }

    return mins;
}


RCDS::RCDS(GenSync::SyncProtocol RCDS_base_proto, size_t terminal_str_size, size_t levels, size_t partition)
        : m_RCDS_base_proto(RCDS_base_proto), m_terminal_str_size(terminal_str_size), m_levels(levels), m_partition(partition) {

    m_single_file_mode = false;
    m_save_file = true;
}

bool RCDS::addElem(shared_ptr<DataObject> newDatum) {
    Logger::gLog(Logger::METHOD,"Entering RCDS::addElem");

    if(!SyncMethod::addElem(newDatum)) return false;
    register_file(newDatum);
    Logger::gLog(Logger::METHOD, "Successfully added shared_ptr<DataObject> {" + newDatum->print() + "}");
    return true;
}

void RCDS::register_file(shared_ptr<DataObject>& str) {
    // multiple folders is not supported yet
    string tmp = str->to_string();
    if (!m_target.empty()) {
        if (m_target != tmp)
            Logger::error_and_quit("Syncing multiple folders is not supported!");
    }
    else
        m_target = tmp;
    if (m_target.empty())
        Logger::error_and_quit("Folder name cannot be empty!");

    if (!isFile((m_target))) {
        while (!m_target.empty() && m_target.back() == '/')
            m_target.pop_back();
        for (const string &f_name : walkRelDir(m_target))
            m_filenames.push_back(make_shared<DataObject>(f_name));
        m_single_file_mode = false;
    } else {
        m_filenames.push_back(make_shared<DataObject>(m_target));
        m_single_file_mode = true;
    }
}

bool RCDS::SyncServer(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                      list<shared_ptr<DataObject>> &otherMinusSelf) {
    Logger::gLog(Logger::METHOD, "Entering RCDS::SyncServer");

    commSync->commListen();

    // check if we are syncing the same thing
    commSync->commSend((m_single_file_mode ? 'F' : 'S'));
    if (commSync->commRecv_byte() != SYNC_OK_FLAG) {
        Logger::error_and_quit("Cannot sync folder and file together!");
    }

    if (m_single_file_mode) {
        Logger::gLog(Logger::METHOD, "We use RCDS.");
        int levels = floor(log10(getFileSize(m_target)));
        int par = 4;
        commSync->commSend(levels);
        string_server(commSync, m_target, levels, par);
    } else {
        if (m_target.back() != '/')
            m_target += "/";

        vector<shared_ptr<DataObject>> unique_set = check_and_get(m_filenames);
        send_diff_files(commSync, 10e2, sizeof(size_t), unique_set, selfMinusOther, otherMinusSelf);
//        newCommunicant->commClose();

        // clear otherMinusSelf
        otherMinusSelf.clear();

        commSync->commSend((long) selfMinusOther.size());
        for (auto &f: selfMinusOther) {
            string filename = m_hash_to_filename[ZZtoT<std::size_t>(f->to_ZZ())];

            commSync->commSend((filename.empty()) ? "E" : filename);
            if (filename.empty())
                continue;

            string full_filename = m_target + filename;
            int mode = 0;
            // receive OK/NO_INFO
            (commSync->commRecv_byte() == SYNC_OK_FLAG) ? mode = 1 : mode = 2;

            // The FAIL sent here temporarily refers to the small file sending method
            // FIXME: just for debug, change back to 500
            if (mode == 1 and getFileSize(full_filename) < 500) {
                commSync->commSend(SYNC_FAIL_FLAG);
                mode = 2;
            } else if (mode == 1)
                commSync->commSend(SYNC_OK_FLAG);

            // we use full sync if the file is small
            if (mode == 2) {
                Logger::gLog(Logger::METHOD, "We use FullSync.");
                if (m_save_file) {
                    string content = scanWholeTxt(full_filename);
                    commSync->commSend((content.empty() ? "E" : content));
                }
            } else if (mode == 1) {
                Logger::gLog(Logger::METHOD, "We use RCDS");
                int levels = nearbyint(log10(getFileSize(full_filename)));
                // FIXME: just for debug, change back to 4
                int par = 4;
                commSync->commSend(levels);
                string_server(commSync, full_filename, levels, par);
            }
            else
                Logger::error_and_quit("Unkonwn Sync Mode, should never happen in RCDS");

        }
    }

    // Record Stats
    mySyncStats.increment(SyncStats::XMIT, commSync->getXmitBytes());
    mySyncStats.increment(SyncStats::RECV, commSync->getRecvBytes());

    commSync->commClose();
    return true;
}

bool RCDS::SyncClient(const shared_ptr<Communicant> &commSync, list<shared_ptr<DataObject>> &selfMinusOther,
                      list<shared_ptr<DataObject>> &otherMinusSelf) {
    Logger::gLog(Logger::METHOD, "Entering RCDS::SyncClient");

    commSync->commConnect();

    // make sure we ar syncing the same type of things
    if (commSync->commRecv_int() != (m_single_file_mode ? 'F' : 'S')) {
        commSync->commSend(SYNC_FAIL_FLAG);
        Logger::error_and_quit("Cannot sync folder and file together!");
    }
    commSync->commSend(SYNC_OK_FLAG);


    if (m_single_file_mode) {
        Logger::gLog(Logger::METHOD, "We use RCDS.");
        int levels = commSync->commRecv_int();
        int par = 4;
        string syncContent = string_client(commSync, m_target, levels, par);
        if (m_save_file)
            writeStrToFile(m_target, syncContent);
    } else {
        if (m_target.back() != '/')
            m_target += "/";

        vector<shared_ptr<DataObject>> unique_set = check_and_get(m_filenames);
        get_diff_files(commSync, 10e2, sizeof(size_t), unique_set, selfMinusOther, otherMinusSelf);
//        newCommunicant->commClose();

        // clear otherMinusSelf
        otherMinusSelf.clear();

        size_t diff_size = commSync->commRecv_long();
        for (int i = 0; i < diff_size; ++ i) {
            // mode 1: I have a file, lets sync.
            // mode 2: i don't have this file, send me the whole thing.
            int mode = 0;
            string filename = commSync->commRecv_string();
            if (filename == "E")
                continue;
            string full_filename = m_target + filename;

            // if file exists, send OK
            if (isPathExist(full_filename)) {
                commSync->commSend(SYNC_OK_FLAG);
                mode = 1;
            }
            // otherwise send NO_INFO
            else {
                commSync->commSend(SYNC_NO_INFO);
                mode = 2;
            }

            // The FAIL receive here temporarily refers to the small file sending method
            if (mode == 1 and commSync->commRecv_byte() == SYNC_FAIL_FLAG)
                mode = 2;

            // we use full sync if the file is small
            if (mode == 2) {
                cout << "Using FullSync" << endl;
                Logger::gLog(Logger::METHOD, "We use FullSync.");
                string content = commSync->commRecv_string();
                if (m_save_file) {
                    writeStrToFile(full_filename, (content == "E" ? "" : content));
                }
            } else if (mode == 1) {
                cout << "Using RCDS" << endl;
                Logger::gLog(Logger::METHOD, "We use RCDS");
                int levels = commSync->commRecv_int();
                // FIXME: just for debug, change back to 4
                int par = 4;

                string syncContent = string_client(commSync, full_filename, levels, par);
                if (m_save_file)
                    writeStrToFile(full_filename, syncContent);
           }
            else
                Logger::error_and_quit("Unknown Sync Mode, should never happen in RCDS");

        }
    }

    // Record Stats
    mySyncStats.increment(SyncStats::XMIT, commSync->getXmitBytes());
    mySyncStats.increment(SyncStats::RECV, commSync->getRecvBytes());

    commSync->commClose();
    return true;
}

void RCDS::set_base_proto(shared_ptr<SyncMethod> &setHost, long mbar, size_t elem_size) {
    if (GenSync::SyncProtocol::InteractiveCPISync == m_RCDS_base_proto)
//            setHost = make_shared<InterCPISync>(5, elem_size * 8, 64, 3, true);
//        else
        setHost = make_shared<InterCPISync>(5, elem_size * 8, 64, 3, true);
//        else if (GenSync::SyncProtocol::InteractiveCPISync == baseSyncProtocol)
//            setHost = make_shared<InterCPISync>(5, elem_size * 8, 64, 3, true);
}

bool RCDS::get_diff_files(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                    vector<shared_ptr<DataObject>> &full_set, list<shared_ptr<DataObject>> &selfMinusOther,
                    list<shared_ptr<DataObject>> &otherMinusSelf) {
    selfMinusOther.clear();
    otherMinusSelf.clear();

    shared_ptr<SyncMethod> setHost;
    SyncMethod::SyncClient(commSync, selfMinusOther, otherMinusSelf);
    set_base_proto(setHost, mbar, elem_size);
    for (auto &dop : full_set) {
        bool ret = setHost->addElem(dop);
        Logger::gLog(Logger::METHOD, to_string(ret));
    }

    Logger::gLog(Logger::METHOD, string("Diff: ") + to_string(full_set.size()));

    return setHost->SyncClient(commSync, selfMinusOther, otherMinusSelf);
}

bool RCDS::send_diff_files(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                     vector<shared_ptr<DataObject>> &full_set, list<shared_ptr<DataObject>> &selfMinusOther,
                     list<shared_ptr<DataObject>> &otherMinusSelf) {
    selfMinusOther.clear();
    otherMinusSelf.clear();

    shared_ptr<SyncMethod> setHost;
    SyncMethod::SyncServer(commSync, selfMinusOther, otherMinusSelf);
    set_base_proto(setHost, mbar, elem_size);
    for (auto &dop : full_set) {
        bool ret = setHost->addElem(dop);
        Logger::gLog(Logger::METHOD, to_string(ret));
    }

    Logger::gLog(Logger::METHOD, string("Diff: ") + to_string(full_set.size()));

    return setHost->SyncServer(commSync, selfMinusOther, otherMinusSelf);
}

bool RCDS::string_server(const shared_ptr<Communicant> &commSync, const string& filename, int level, int partition) {
    auto stringHost = RCDS_Synchronizer(m_RCDS_base_proto, level, partition);

    string content = scanWholeTxt(filename);
    stringHost.add_str(move(content));

    stringHost.SyncServer(commSync);
    return true;
}

string RCDS::string_client(const shared_ptr<Communicant> &commSync, const string& filename, int level, int partition) {
    auto stringHost = RCDS_Synchronizer(m_RCDS_base_proto, level, partition);

    list<shared_ptr<DataObject>> selfMinusOther, otherMinusSelf;

    string content = scanWholeTxt(filename);
#ifdef RCDS_RECORD_OPERATIONS_TIME
    auto start_pq = std::chrono::high_resolution_clock::now();
#endif
    stringHost.add_str(move(content));
#ifdef RCDS_RECORD_OPERATIONS_TIME
    auto end_pq = std::chrono::high_resolution_clock::now();
    size_t total_time = std::chrono::duration_cast<std::chrono::microseconds>(end_pq - start_pq).count();
    cout << total_time << " ";
#endif

    stringHost.SyncClient(commSync);

    // recover string
    string res;
#ifdef RCDS_RECORD_OPERATIONS_TIME
    start_pq = std::chrono::high_resolution_clock::now();
#endif
    stringHost.recover_str(res);
#ifdef RCDS_RECORD_OPERATIONS_TIME
    end_pq = std::chrono::high_resolution_clock::now();
    total_time = std::chrono::duration_cast<std::chrono::microseconds>(end_pq - start_pq).count();
    cout << total_time << " ";
#endif
    return res;
}