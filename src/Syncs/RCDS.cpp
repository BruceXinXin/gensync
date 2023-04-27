//
// Created by bruce on 2/16/2023.
//

#include <cassert>
#include <unordered_map>

#include <CPISync/Syncs/RCDS.h>

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

    if (level_num <= 0)
        Logger::error_and_quit("Level cannot <= 0!");

    init_tree_by_string_data();

    tree_shingles.clear();
    for (auto& item : get_unique_shingleZZs_from_tree())
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
//            if (success || GenSync::SyncProtocol::IBLTSyncSetDiff != base_sync_protocol)
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

    bool success = false;
    list<shared_ptr<DataObject>> selfMinusOther, otherMinusSelf;
    size_t top_mbar = pow(2 * partition_num, level_num) * 2; // Upper bound on the number of symmetrical difference
    while (!success and mbar < top_mbar) { // if set recon failed, This can be caused by error rate and small mbar
        success = sync_tree_shingles_client(commSync, mbar, sizeof(shingle), selfMinusOther, otherMinusSelf);
        commSync->commSend(success? SYNC_SUCCESS: SYNC_FAILURE);
        success &= (SYNC_SUCCESS == commSync->commRecv_int());
//        if (success || GenSync::SyncProtocol::IBLTSyncSetDiff != base_sync_protocol)
        if (success)
            break;

        Logger::gLog(Logger::METHOD,
                     "RCDS_Synchronizer::SyncClient - mbar doubled from " + to_string(mbar) + " to " +
                     to_string(2 * (mbar + 1)));
        mbar = 2 * (mbar + 1);
    }

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

    // 处理elements, 加入oms, 去除smo(Server端暂时无此需求)
    tree_shingles.reserve(tree_shingles.size() + otherMinusSelf.size());
    for (auto& oms: otherMinusSelf)
        tree_shingles.emplace_back(move(oms));
    del_elements_from_set_pointers(selfMinusOther);

    return success;
}

bool RCDS_Synchronizer::recover_str(shared_ptr<DataObject>& recovered_str) {
    hash_shingle_tree.clear();
    hash_shingle_tree.resize(level_num);
    for (const auto& s_zz : tree_shingles) {
        shingle s = ZZtoT<shingle>(s_zz->to_ZZ());
        hash_shingle_tree[s.level].insert(s);
    }

    data = retrieve_string();
    recovered_str = make_shared<DataObject>(data);
    return true;
}

string RCDS_Synchronizer::retrieve_string() {
    // retrieve string from bottom
    string substring;
    for (int i = hash_shingle_tree.size() - 2; i >= 0; -- i) {
        for (const auto& shingle : hash_shingle_tree[i]) {
            auto it = cycle_query.find(shingle.second);
            if (it != cycle_query.end()) {
                vector<size_t> temp;
                substring.clear();
                auto& tmp_cycle = it->second;

                if (!shingles_to_substr_hashes(tmp_cycle, i + 1, temp)) {
                    substring = get_str_from_dict_by_hash(shingle.second);
                    cerr << "shingles_to_substr_hashes() return false." << endl;
                }

                for (size_t hash: temp) {
                    if (dictionary.find(hash) == dictionary.end())
                        // TODO: exit?
                        cerr << "Recover may have failed - Dictionary lookup failed for " << hash << " at level "
                             << shingle.level << endl;
                    substring += get_str_from_dict_by_hash(hash);
                }
                add_str_to_dict(substring);
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
        // 将每一段由local_min分隔的字符串加入哈希
        for (size_t min: local_mins) {
            min += p.first;
            hash_set.push_back(add_str_to_dict_by_idx_len(prev, min - prev));
            prev = min;
        }
        hash_set.push_back(add_str_to_dict_by_idx_len(prev, p.second - (prev - p.first)));
    }

    // write it to cyc-dict
    auto cyc_it = hash_to_substr_hashes.find(str_hash);
    if (cyc_it == hash_to_substr_hashes.end()) // 当cyc[str_hash]对应的vector只有str_hash一个值时, 可以直接
        hash_to_substr_hashes[str_hash] = hash_set; // update cyc_dict
    else if (cyc_it->second != hash_set) {
        if (cyc_it->second.size() == 1 && cyc_it->second.front() == cyc_it->first) // just this string alone
            hash_to_substr_hashes[str_hash] = hash_set; // update cyc_dict
        else // It is overwritten
            Logger::error_and_quit("More than one answer is possible for cyc_dict!");
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

    size_t shingle_size = 2 * pow(shingle_sz_divisor, level_num); // size of window
    if (shingle_size < 1)
        Logger::error_and_quit("Shingle size cannot < 1!");
    size_t space = 4 * pow(space_sz_divisor, level_num); // hash space
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
        space = floor(space / space_sz_divisor);
        shingle_size = floor(shingle_size / shingle_sz_divisor);
    }
}

void RCDS_Synchronizer::gen_queries(const list<shared_ptr<DataObject>> &otherMinusSelf) {
    cycle_query.clear();
    terminal_query.clear();

    std::unordered_set<size_t> tmp;
    for (const auto& shingle_zz: otherMinusSelf) {
        auto s = ZZtoT<shingle>(shingle_zz->to_ZZ());
        if (tmp.emplace(s.second).second) // lower level is preferred
            cycle_query.erase(s.second);

        if (dictionary.find(s.second) == dictionary.end()) { // cannot find in dict
            // shingle不是最后一层
            if (s.level < level_num - 1)
                cycle_query.emplace(s.second, cycle{0, 0, 0});
                // 最后一层
            else
                terminal_query.emplace(s.second, "");
        }
    }
}

bool RCDS_Synchronizer::answer_queries(std::unordered_set<size_t> &queries) {
    cycle_concern.clear();
    terminal_concern.clear();

    // from bottom to top
    for (auto rit = hash_shingle_tree.rbegin(); rit != hash_shingle_tree.rend(); ++ rit) {
        for (const auto& shingle : *rit) {
            auto it = queries.find(shingle.second);
            if (it != queries.end()) {
                // shingle是最后一层
                if (level_num - 1 == shingle.level)
                    terminal_concern.emplace(shingle.second, get_str_from_dict_by_hash(shingle.second));
                    // 不是最后一层
                else {
                    // auto也是对的，证明不一定需要修改cyc_dict[shingle.second]才正确
                    auto& tmp_vec = hash_to_substr_hashes[shingle.second];
                    cycle tmp = cycle{.head = tmp_vec.front(), .len = static_cast<uint32_t>(tmp_vec.size()), .backtracking_time=0};

                    if (shingles_to_substr_hashes(tmp, shingle.level + 1, tmp_vec))
                        cycle_concern[shingle.second] = tmp;
                    else
                        continue; // failed
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

    // 取[i - 1]和[i]两个
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

    // 优先找cur, 然后再去org找
    if (curr != cur.end()) {
        for (const auto& tmp_shingle: curr->second)
            if (tmp_shingle.occur_time > 0)
                res_vec.push_back(tmp_shingle);
    }
    else {
        auto cur_it = org.find(point);
        // org没东西, 就不用往下找了
        if (cur_it == org.end())
            return res_vec;

        for (const auto& tmp_shingle: cur_it->second)
            if (tmp_shingle.occur_time > 0)
                res_vec.push_back(tmp_shingle);
    }

    return res_vec;
}

bool RCDS_Synchronizer::shingles_to_substr_hashes(cycle &cyc_info, int level, vector<size_t>& hashes_vec) const  {
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
//            last_edge = get_nxt_edge(cur_edge, next_shingles.back());
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
//            last_edge = get_nxt_edge(cur_edge, next_edges_stk.back().back());
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
//                last_edge = get_nxt_edge(cur_edge, next_edges_stk.back().back());
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

//        if (strCollect_size == cyc_info.cyc && cyc_info.cyc != 0) {
//            return true;
//        }
    }
    return false;
}

std::map<size_t, vector<shingle>> RCDS_Synchronizer::tree_level_to_shingle_dict(int level) const {
    std::map<size_t, vector<shingle>> res;
    for (const auto& shingle : hash_shingle_tree[level])
        res[shingle.first].push_back(shingle);

    for (auto &shingle: res)
        std::sort(shingle.second.begin(), shingle.second.end());
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
        // 按test方法改了参数
//        setHost = make_shared<InterCPISync>(5, elem_size * 8, 64, 3, true);
        setHost = make_shared<InterCPISync>(2 * UCHAR_MAX, pow((double) sizeof(randZZ()), 2.0), 8, 5, true);
//    if (GenSync::SyncProtocol::CPISync == base_sync_protocol)
//        // 按test方法改了参数
////        setHost = make_shared<ProbCPISync>(mbar, elem_size * 8, 64, true);
//        setHost = make_shared<CPISync>(2 * UCHAR_MAX, elem_size * 8, 8, false);
}

bool RCDS_Synchronizer::sync_tree_shingles_client(const shared_ptr<Communicant> &commSync, long mbar, size_t elem_size,
                                                  list<shared_ptr<DataObject>> &selfMinusOther, list<shared_ptr<DataObject>> &otherMinusSelf) {
    selfMinusOther.clear();
    otherMinusSelf.clear();
    shared_ptr<SyncMethod> setHost;
    // 取消多态, 抽出这句有用的出来
    commSync->resetCommCounters();
    configure(setHost, mbar, elem_size);
    for (auto& dop : tree_shingles) {
        setHost->addElem(dop); // Add to GenSync
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
    // 取消多态, 抽出这句有用的出来
    commSync->resetCommCounters();
    configure(setHost, mbar, elem_size);
    for (const auto& dop : tree_shingles)
        setHost->addElem(dop); // Add to GenSync
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


RCDS::RCDS(GenSync::SyncProtocol RCDS_base_proto, size_t terminal_str_size, size_t levels, size_t partition)
        : m_RCDS_base_proto(RCDS_base_proto), m_terminal_str_size(terminal_str_size), m_levels(levels), m_partition(partition) {

    // 多文件模式下不会调用Client的addStr, 也就不会更新这个标志, 先置位单文件模式为false
    m_single_file_mode = false;
    m_save_file = true;
}

bool RCDS::addElem(shared_ptr<DataObject> newDatum) {
    Logger::gLog(Logger::METHOD,"Entering RCDS::addElem");

    if(!SyncMethod::addElem(newDatum)) return false;
    addStr(newDatum);
    Logger::gLog(Logger::METHOD, "Successfully added shared_ptr<DataObject> {" + newDatum->print() + "}");
    return true;
}

// 把文件名字符串以DataObject形式放入setPointers(同时更新了FolderName), 并判断是否是singleFileMode
void RCDS::addStr(shared_ptr<DataObject>& str) {
    // 不允许多个文件夹
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

    // 单文件模式必须在Sync之前就写好双方的FolderName, 多文件模式可以只写服务端的FolderName
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

        // 清空otherMinusSelf
        otherMinusSelf.clear();

        commSync->commSend((long) selfMinusOther.size());
        for (auto &f: selfMinusOther) {
            string filename = m_hash_to_filename[ZZtoT<std::size_t>(f->to_ZZ())];

            commSync->commSend((filename.empty()) ? "E" : filename);
            if (filename.empty())
                continue;

            string full_filename = m_target + filename;
            int mode = 0;
            // 接收OK/NO_INFO
            (commSync->commRecv_byte() == SYNC_OK_FLAG) ? mode = 1 : mode = 2;

            // 这里发的FAIL临时指代小文件发送方式
            // FIXME: just for debug, change back to 500
            if (mode == 1 and getFileSize(full_filename) < 1) {
                commSync->commSend(SYNC_FAIL_FLAG);
                mode = 2;
            } else if (mode == 1) {
                commSync->commSend(SYNC_OK_FLAG);// check file size
            }

            // 小文件用full sync
            if (mode == 2) {
                Logger::gLog(Logger::METHOD, "We use FullSync.");
                if (m_save_file) {
                    string content = scanTxtFromFile(full_filename, numeric_limits<int>::max());
                    commSync->commSend((content.empty() ? "E" : content));
                }
            } else if (mode == 1) {
                Logger::gLog(Logger::METHOD, "We use RCDS");
                // FIXME: just for debug, change back to log10
                int levels = nearbyint(log10(getFileSize(full_filename)));
                // FIXME: just for debug, change back to 4
                int par = 4;
                commSync->commSend(levels);
                string_server(commSync, full_filename, levels, par);
            }
            else Logger::error_and_quit("Unkonwn Sync Mode, should never happen in RCDS");

        }
    }
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

        // 清空otherMinusSelf
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
//            cout << full_filename << endl;
            // 存在此文件, 发OK
            if (isPathExist(full_filename)) {
                commSync->commSend(SYNC_OK_FLAG);
                mode = 1;
            } else { // 不存在, 发NO_INFO
                commSync->commSend(SYNC_NO_INFO);
                mode = 2;
            }

            // 这里收的FAIL临时指代小文件发送方式
            if (mode == 1 and commSync->commRecv_byte() == SYNC_FAIL_FLAG)
                mode = 2;

            // 小文件用full sync
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

    commSync->commClose();
    return true;
}

void RCDS::set_base_proto(shared_ptr<SyncMethod> &setHost, long mbar, size_t elem_size) {
    if (GenSync::SyncProtocol::InteractiveCPISync == m_RCDS_base_proto)
//            setHost = make_shared<InterCPISync>(5, elem_size * 8, 64, 3, true);
//        else if (GenSync::SyncProtocol::CPISync == baseSyncProtocol)
////        else
//            setHost = make_shared<ProbCPISync>(mbar, elem_size * 8, 64, true);
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
        bool ret = setHost->addElem(dop); // Add to GenSync
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
        bool ret = setHost->addElem(dop); // Add to GenSync
        Logger::gLog(Logger::METHOD, to_string(ret));
    }

    Logger::gLog(Logger::METHOD, string("Diff: ") + to_string(full_set.size()));

    return setHost->SyncServer(commSync, selfMinusOther, otherMinusSelf);
}

bool RCDS::string_server(const shared_ptr<Communicant> &commSync, const string& filename, int level, int partition) {
    // 必须要让useExisting为true!
    auto stringHost = RCDS_Synchronizer(m_RCDS_base_proto, level, partition);

    string content = scanTxtFromFile(filename, numeric_limits<int>::max());
    stringHost.add_str(move(content));

    stringHost.SyncServer(commSync);
    return true;
}

string RCDS::string_client(const shared_ptr<Communicant> &commSync, const string& filename, int level, int partition) {
    // 必须要让useExisting为true!
    auto stringHost = RCDS_Synchronizer(m_RCDS_base_proto, level, partition);

    list<shared_ptr<DataObject>> selfMinusOther, otherMinusSelf;

    string content = scanTxtFromFile(filename, numeric_limits<int>::max());
    stringHost.add_str(move(content));

    stringHost.SyncClient(commSync);

    // 重建字符串
    shared_ptr<DataObject> res;
    stringHost.recover_str(res);
    return res->to_string();
}