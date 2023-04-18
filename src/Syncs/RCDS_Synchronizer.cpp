//
// Created by bruce on 2/16/2023.
//

#include <CPISync/Syncs/RCDS_Synchronizer.h>

RCDS_Synchronizer::RCDS_Synchronizer(size_t terminal_str_size, size_t levels, size_t partition,
                                     GenSync::SyncProtocol base_set_proto, size_t shingle_size, size_t space)
        : terminalStrSz(terminal_str_size), level_num(levels), partition_num(partition), baseSyncProtocol(base_set_proto),
          shingle_sz(shingle_size), space_sz(space) {
    // 取消多态
//    SyncID = SYNC_TYPE::RCDS_Synchronizer;
    if (levels > USHRT_MAX or levels < 2)
        throw invalid_argument("Num of Level specified should be between 2 and " + to_string(USHRT_MAX));
    useExistingConnection = false;
//    initResources(initRes);
}

string RCDS_Synchronizer::retrieve_string() {
    // retrieve string from bottom
    string substring;
    for (int i = hashShingleTree.size() - 2; i >= 0; -- i) {
        for (const auto& shingle : hashShingleTree[i]) {
            auto it = cyc_query.find(shingle.second);
            if (it != cyc_query.end()) {
                vector<size_t> temp;
                substring.clear();
                auto& tmp_cycle = it->second;

                if (!shingle2hash_train(tmp_cycle, hashShingleTree[i + 1], temp))
                    substring = dict_getstr(shingle.second);

                for (size_t hash: temp) {
                    if (dictionary.find(hash) == dictionary.end())
                        // TODO: exit?
                        cerr << "Recover may have failed - Dictionary lookup failed for " << hash << " at level "
                             << shingle.level << endl;
                    substring += dict_getstr(hash);
                }
                add_str_to_dictionary(substring);
            }
        }
    }

    return substring.empty()? data: substring;
}

bool RCDS_Synchronizer::add_str(shared_ptr<DataObject>& str) {
    Logger::gLog(Logger::METHOD,
                 "Entering RCDS_Synchronizer::addStr. Parameters - num of par: " + to_string(partition_num) + ", num of lvls: "
                 + to_string(level_num) + ", Terminal String Size: " + to_string(terminalStrSz) + ", Actual String Size: " +
                 to_string(data.size()));

    data = str->to_string();

    if (data.empty()) return false;

    if (level_num <= 0)
        Logger::error_and_quit("Level cannot > 0!");

    go_through_tree();

    setPointers.clear();
    // 加了引用
    for (auto& item : getHashShingles_ZZ())
        setPointers.emplace_back(new DataObject(item));
    return true;
}

bool RCDS_Synchronizer::SyncServer(const shared_ptr<Communicant> &commSync) {
    Logger::gLog(Logger::METHOD, "Entering RCDS_Synchronizer::SyncServer");
    if (!useExistingConnection) {
        Logger::gLog(Logger::METHOD, "Chose not use existing connection.");
        commSync->commListen();
        RecvSyncParam(commSync);
        Logger::gLog(Logger::METHOD, "Sync params success!");
    }

    long mbar = 0;
    if (GenSync::SyncProtocol::IBLTSyncSetDiff == baseSyncProtocol) {
        StrataEst est = StrataEst(sizeof(shingle));

        for (auto& item: setPointers)
            est.insert(item);

        // send strata estimator
        // save more memory
        {
            size_t numSize = (size_t) commSync->commRecv_long();

            vector<IBLT> theirs;

            for(int ii = 0; ii < numSize; ++ii) {
                theirs.push_back(commSync->commRecv_IBLT());
            }

            est -= {theirs};
        }
        mbar = est.estimate();

        commSync->commSend(mbar);
    }
    else if (GenSync::SyncProtocol::CPISync == baseSyncProtocol) { // Not well supported
        mbar = 1 << 10;
    }

    bool success = false;
    {
        size_t top_mbar = pow(2 * partition_num, level_num) * 2; // Upper bound on the number of symmetrical difference
        // If failed, try bigger mbar, inspired by Bowen
        list<shared_ptr<DataObject>> selfMinusOther, otherMinusSelf;
        while (!success && mbar < top_mbar) {
            success = setReconServer(commSync, mbar, sizeof(shingle), setPointers, selfMinusOther, otherMinusSelf);
            success &= (SYNC_SUCCESS == commSync->commRecv_int());
            commSync->commSend(success ? SYNC_SUCCESS : SYNC_FAILURE);
            if (success || GenSync::SyncProtocol::IBLTSyncSetDiff != baseSyncProtocol)
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
                 "Answered " + to_string(cyc_concern.size()) + " cycles and " +
                 to_string(term_concern.size()) + " hashes.");

    for (const auto& dic : term_concern) {
        string tmp_str = dict_getstr(dic.first);
        commSync->commSend(tmp_str.empty()? "E": tmp_str);
    }

    for (const auto& cyc : cyc_concern)
        commSync->commSend(TtoZZ(cyc.second), sizeof(cycle));

    Logger::gLog(Logger::METHOD, "Server RCDS Sync part finishes!");
    if (!useExistingConnection)
        commSync->commClose();

    return success;
}

bool RCDS_Synchronizer::SyncClient(const shared_ptr<Communicant> &commSync) {

    Logger::gLog(Logger::METHOD, "Entering RCDS_Synchronizer::SyncClient");
    if (!useExistingConnection) {
        Logger::gLog(Logger::METHOD, "Chose not use existing connection.");
        commSync->commConnect();
        SendSyncParam(commSync);
        Logger::gLog(Logger::METHOD, "Sync params success!");
    }

    long mbar = 0;
    if (GenSync::SyncProtocol::IBLTSyncSetDiff == baseSyncProtocol) {
        StrataEst est = StrataEst(sizeof(shingle));

        for (auto& item :setPointers)
            est.insert(item); // Add to estimator

        const auto& strata = est.getStrata();
        commSync->commSend((long) strata.size());

        // Access the iblt to serialize it
        for (const IBLT &iblt : strata)
            commSync->commSend(iblt, false);

        mbar = commSync->commRecv_long();

    } else if (GenSync::SyncProtocol::CPISync == baseSyncProtocol) {
        mbar = 1 << 10;
    }

    bool success = false;
    list<shared_ptr<DataObject>> selfMinusOther, otherMinusSelf;
    size_t top_mbar = pow(2 * partition_num, level_num) * 2; // Upper bound on the number of symmetrical difference
    while (!success and mbar < top_mbar) { // if set recon failed, This can be caused by error rate and small mbar
        success = setReconClient(commSync, mbar, sizeof(shingle), setPointers, selfMinusOther, otherMinusSelf);
        commSync->commSend(success? SYNC_SUCCESS: SYNC_FAILURE);
        success &= (SYNC_SUCCESS == commSync->commRecv_int());
        if (success || GenSync::SyncProtocol::IBLTSyncSetDiff != baseSyncProtocol)
            break;

        Logger::gLog(Logger::METHOD,
                     "RCDS_Synchronizer::SyncClient - mbar doubled from " + to_string(mbar) + " to " +
                     to_string(2 * (mbar + 1)));
        mbar = 2 * (mbar + 1);
    }

    // before the query
    gen_queries(otherMinusSelf);

    // send queries
    commSync->commSend(cyc_query.size() + term_query.size());
    // ask about cycles
    for (const auto& cyc: cyc_query)
        commSync->commSend(cyc.first);
    // ask about terms
    for (const auto& term: term_query)
        commSync->commSend(term.first);

    // add to dict
    for (int i = 0; i < term_query.size(); ++ i) {
        auto tmp = commSync->commRecv_string();
        if (tmp != "E") {
            // non-empty string
            cyc_query.erase(add_str_to_dictionary(tmp));
        }
    }

    for (auto &cyc: cyc_query)
        cyc.second = ZZtoT<cycle>(commSync->commRecv_ZZ(sizeof(cycle)));

    Logger::gLog(Logger::METHOD, "Server RCDS Sync part finishes!");
    if (!useExistingConnection)
        commSync->commClose();

    // 处理elements, 加入oms, 去除smo(Server端暂时无此需求)
    for (auto& oms: otherMinusSelf)
        setPointers.emplace_back(move(oms));
    RCDS_Synchronizer::delGroup(setPointers, selfMinusOther);

    return success;
}

bool RCDS_Synchronizer::recover_str(shared_ptr<DataObject>& recovered_str) {
    hashShingleTree.clear();
    hashShingleTree.resize(level_num);
    for (const auto& s_zz : setPointers) {
        shingle s = ZZtoT<shingle>(s_zz->to_ZZ());
        hashShingleTree[s.level].insert(s);
    }

    data = retrieve_string();
    recovered_str = make_shared<DataObject>(data);
    return true;
}

vector<size_t> RCDS_Synchronizer::create_hash_vec(size_t str_hash, size_t space, size_t shingle_size) {
    vector<size_t> hash_val, hash_set;
    auto p = dict_geti(str_hash);
    if (p.second == 0)
        return hash_set;
    auto str = dict_getstr(str_hash);
    size_t win_size = floor((p.second / partition_num) / 2);

    // substring size should not smaller than terminal string size
    if (p.second <= terminalStrSz) {
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
            hash_set.push_back(add_i_to_dictionary(prev, min - prev));
            prev = min;
        }
        hash_set.push_back(add_i_to_dictionary(prev, p.second - (prev - p.first)));
    }

    // write it to cyc-dict
    auto cyc_it = cyc_dict.find(str_hash);
    if (cyc_it == cyc_dict.end()) // 当cyc[str_hash]对应的vector只有str_hash一个值时, 可以直接
        cyc_dict[str_hash] = hash_set; // update cyc_dict
    else if (cyc_it->second != hash_set) {
        if (cyc_it->second.size() == 1 and cyc_it->second.front() == cyc_it->first) // last stage no partition
            cyc_dict[str_hash] = hash_set; // update cyc_dict
        else // It is overwritten
            Logger::error_and_quit("More than one answer is possible for cyc_dict");
    }

    return hash_set;
}


void RCDS_Synchronizer::go_through_tree() {
    hashShingleTree.clear(); // should be redundant

    auto supposed_str_len = pow(partition_num, level_num) * terminalStrSz; // supposed string size

    if (supposed_str_len < 1)
        Logger::error_and_quit(
                "Error params! level_num: " + to_string(level_num) + ", terminalStrSz: "
                + to_string(terminalStrSz) + ", actual string size: " + to_string(data.size()));

    size_t shingle_size = 2 * pow(shingle_sz, level_num); // size of window
    if (shingle_size < 1)
        Logger::error_and_quit("Shingle size cannot < 1!");
    size_t space = 4 * pow(space_sz, level_num); // hash space

    vector<size_t> cur_level;
    hashShingleTree.resize(level_num);

    // level 0
    update_tree_shingles({add_i_to_dictionary(0, data.size())}, 0);

    // level [1, level_num)
    for (int level = 1; level < level_num; ++ level) {
        // get hash vec per level and update the corresponding level
        for (auto substr_hash: unique_substr_hash(hashShingleTree[level - 1])) {
            cur_level = create_hash_vec(substr_hash, space, shingle_size);
            update_tree_shingles(cur_level, level);
        }
        space = floor(space / space_sz);
        shingle_size = floor(shingle_size / shingle_sz);
    }
}

void RCDS_Synchronizer::gen_queries(list<shared_ptr<DataObject>> &otherMinusSelf) {
    term_query.clear();
    cyc_query.clear();

    std::unordered_set<size_t> tmp;
    for (auto& shingle_zz: otherMinusSelf) {
        auto s = ZZtoT<shingle>(shingle_zz->to_ZZ());
        if (tmp.emplace(s.second).second) // lower level is preferred
            cyc_query.erase(s.second);

        if (dictionary.find(s.second) == dictionary.end()) { // cannot find in dict
            // shingle不是最后一层
            if (s.level < level_num - 1)
                cyc_query.emplace(s.second, cycle{0, 0, 0});
            // 最后一层
            else
                term_query.emplace(s.second, "");
        }
    }
}

bool RCDS_Synchronizer::answer_queries(std::unordered_set<size_t> &queries) {
    cyc_concern.clear();
    term_concern.clear();

    // from bottom to top
    for (auto rit = hashShingleTree.rbegin(); rit != hashShingleTree.rend(); ++ rit) {
        for (auto shingle : *rit) {
            auto it = queries.find(shingle.second);
            if (it != queries.end()) {
                // shingle是最后一层
                if (level_num - 1 == shingle.level)
                    term_concern.emplace(shingle.second, dict_getstr(shingle.second));
                // 不是最后一层
                else {
                    auto& tmp_vec = cyc_dict[shingle.second];
                    cycle tmp = cycle{.head = tmp_vec.front(), .len = (unsigned int) tmp_vec.size(), .cyc=0};

                    if (shingle2hash_train(tmp, hashShingleTree[shingle.level + 1], cyc_dict[shingle.second]))
                        cyc_concern[shingle.second] = tmp;
                    else
                        continue;
                }

                // if success, erase this query
                queries.erase(it);
            }
        }
    }
    return queries.empty();
}

void RCDS_Synchronizer::update_tree_shingles(const vector<size_t>& hash_vector, uint16_t level) {
    if (hashShingleTree.size() <= level)  Logger::error_and_quit("We have exceeded the levels of the tree");
    if (hash_vector.size() > 100)
        cout << "It is advised to not exceed 100 partitions for fast backtracking at Level: " + to_string(level) +
                " Current set size: " + to_string(hash_vector.size()) << endl;
    if (hash_vector.empty()) return;


    map<pair<size_t, size_t>, size_t> tmp;

    // 取[i - 1]和[i]两个
    // make shingles including a number of start shingles size of shingle size - 1
    for (int i = 0; i < hash_vector.size(); ++i) {
        size_t shingle;
        (i > 0) ? shingle = hash_vector[i - 1] : shingle = 0;

        if (tmp.find({shingle, hash_vector[i]}) != tmp.end())
            tmp[{shingle, hash_vector[i]}]++;
        else
            tmp[{shingle, hash_vector[i]}] = 1;
    }
    if (tmp.empty())
        throw invalid_argument(
                "update_tree shingle is empty");

    for (auto item = tmp.begin(); item != tmp.end(); ++item) {
        if (item->second > USHRT_MAX)
            Logger::error_and_quit(
                    "Shingle occurrance is larger than USHRT_MAX, (backtracking could be infeasiable and our shingle_hash carrier is overflown)");
        hashShingleTree[level].insert(
                shingle{item->first.first, item->first.second, level, (uint16_t) item->second});
    }

}

vector<shingle> RCDS_Synchronizer::get_nxt_shingle_vec(const size_t cur_edge,
                                                       const map<size_t, vector<shingle>> &last_state_stack,
                                                       const map<size_t, vector<shingle>> &original_state_stack) {
    vector<shingle> res_vec;

    auto from_stateStack = last_state_stack.find(cur_edge);

    // 优先找from_stateStack, 然后再去original_state_stack找
    if (from_stateStack != last_state_stack.end()) {
        for (auto tmp_shingle: from_stateStack->second) {
            if (tmp_shingle.occur_time > 0) res_vec.push_back(tmp_shingle);
        }
    } else {
        auto cur_it = original_state_stack.find(cur_edge);
        if (cur_it == original_state_stack.end())return res_vec; // there is no possible edge(no edge after this)
        for (auto tmp_shingle: cur_it->second) {
            if (tmp_shingle.occur_time > 0) res_vec.push_back(tmp_shingle);
        }
    }

    return res_vec;
}

// functions for backtracking using front, length, and cycle number
bool RCDS_Synchronizer::shingle2hash_train(cycle &cyc_info, const std::set<shingle> &shingle_set,
                                           vector<size_t> &final_str) {

    map<size_t, vector<shingle>> original_state_stack = tree2shingle_dict(
            shingle_set); // get a shingle dict from a level of a tree for fast next edge lookup

    if (shingle_set.empty())
        Logger::error_and_quit(
                "the shingle_set provided is empty for shingle2hash_train, we accessed a tree level with no shingle.");

    vector<map<size_t, vector<shingle>>> stateStack;
    vector<vector<shingle>> nxtEdgeStack;
    stateStack.push_back(
            map<size_t, vector<shingle>>());// Init Original state - nothing changed, state stack only records what is changed
    size_t strCollect_size = 0, curEdge = 0;
    vector<size_t> str; // temprary string hash train to last be compared/placed in final_str


    for (auto head_shingles : original_state_stack[(size_t) 0]) {
        if (cyc_info.head == head_shingles.second) {
            str.push_back(head_shingles.second);
            curEdge = head_shingles.second;
            break;
        }
    }


    if (cyc_info.cyc == 0) { // find head from "final_str" (we are finding cycle number)
        //if we only have one, then we are done
        if (final_str.size() == 1 && cyc_info.len == 1) {
            cyc_info.cyc = 1;
            final_str = str;
            return true;
        }
    } else if (cyc_info.cyc > 0) {// find head from "final_str" (we are retrieving the string from cycle number)
        if (cyc_info.cyc == 1 && cyc_info.len == 1) {
            final_str = str;
            return true;
        }
    }
    //    Resources initRes;
//    initResources(initRes); // initiate Recourses tracking


    shingle last_edge;

    while (!stateStack.empty() and stateStack.size() == nxtEdgeStack.size() + 1) { // while state stack is not empty
        vector<shingle> nxtEdges = get_nxt_shingle_vec(curEdge, stateStack.back(), original_state_stack);

        if (!nxtEdges.empty() and str.size() < cyc_info.len) { // If we can go further with this route
            last_edge = get_nxt_edge(curEdge, nxtEdges.back());
            nxtEdges.pop_back();
            nxtEdgeStack.push_back(nxtEdges);
        } else if (!nxtEdgeStack.empty() and stateStack.size() == nxtEdgeStack.size() + 1 and
                   !nxtEdgeStack.back().empty()) { //if this route is dead, we should look for other options
            if (!str.empty()) str.pop_back();

            //look for other edge options
            last_edge = get_nxt_edge(curEdge, nxtEdgeStack.back().back());
            nxtEdgeStack.back().pop_back();

            stateStack.pop_back();
        } else if (!stateStack.empty() and stateStack.size() == nxtEdgeStack.size() + 1 and !nxtEdgeStack.empty() and
                   nxtEdgeStack.back().empty()) {// if this state is dead and we should look back a state
            if (!str.empty()) str.pop_back();
            // look back a state or multiple if we have empty nxt choice (unique nxt edge)
            while (!nxtEdgeStack.empty() and nxtEdgeStack.back().empty()) {
                nxtEdgeStack.pop_back();
                stateStack.pop_back();
                if (!str.empty()) str.pop_back();
            }
            if (nxtEdgeStack.empty()) {
                return false;
            } else if (!nxtEdgeStack.back().empty()) {
                last_edge = get_nxt_edge(curEdge, nxtEdgeStack.back().back());
                nxtEdgeStack.back().pop_back();
                stateStack.pop_back();
            }
        } else if (stateStack.size() != nxtEdgeStack.size() + 1) {
            Logger::error_and_quit("state stack and nxtEdge Stack size miss match" + to_string(stateStack.size())
                                   + ":" + to_string(nxtEdgeStack.size()));
        }

        str.push_back(curEdge);

        // Change and register our state for shingle occurrence and nxt edges
        map<size_t, vector<shingle>> tmp_stack = stateStack.back();
        bool found = false;
        auto from_stateStack = tmp_stack.find(last_edge.first);
        if (from_stateStack != tmp_stack.end()) {// it is in the state_stack(previously touched shingle)
            for (auto &tmp_shingle: from_stateStack->second) {
                if (tmp_shingle == last_edge) {
                    tmp_shingle.occur_time--;
                    found = true;
                    break;
                }
            }
        }

        if (!found) { // it is never touched,. fetch from original state
            for (auto tmp_shingle: original_state_stack[last_edge.first]) {
                if (tmp_shingle == last_edge) {
                    tmp_shingle.occur_time--;
                }
                tmp_stack[tmp_shingle.first].push_back(tmp_shingle);
            }
        }

        stateStack.push_back(tmp_stack);

//
//        if (!resourceMonitor( initRes, MAX_TIME, MAX_VM_SIZE))
//            return false;

        // if we reached a stop point
        if (str.size() == cyc_info.len) {
            strCollect_size++;
            if (str == final_str || (strCollect_size == cyc_info.cyc and cyc_info.cyc != 0)) {
                cyc_info.cyc = strCollect_size;
                final_str = str;
                // 没有使用, 故删去
//                auto old_mem = initRes.VmemUsed;
//                resourceMonitor(initRes, 300, SIZE_T_MAX);
                // 没有使用, 故删去
//                (old_mem < initRes.VmemUsed) ? highwater = initRes.VmemUsed : highwater = old_mem;
            }
        }


        if (strCollect_size == cyc_info.cyc && cyc_info.cyc != 0) {
            //HeapProfilerStop();
            return true;
        }
    }
    return false;
}

std::map<size_t, vector<shingle>> RCDS_Synchronizer::tree2shingle_dict(const std::set<shingle> &tree_lvl) {
    // prepare shingle_set in a map, and microsorted(sorted for shingles with same head)
    std::map<size_t, vector<shingle>> res;
    for (shingle shingle : tree_lvl) {
        res[shingle.first].push_back(shingle);
    }


    for (auto &shingle : res) {
        std::sort(shingle.second.begin(), shingle.second.end());
    }
    return res;
}

shingle RCDS_Synchronizer::get_nxt_edge(size_t &current_edge, shingle _shingle) {
    current_edge = _shingle.second;
    return _shingle;
}


void RCDS_Synchronizer::SendSyncParam(const shared_ptr<Communicant> &commSync, bool oneWay) {
    Logger::gLog(Logger::METHOD, "Entering SendSyncParam::SendSyncParam");
    // take care of parent sync method for sync mode
//    SyncMethod::SendSyncParam(commSync);
    // 暂时取消互传SyncID
//    commSync->commSend(enumToByte(SyncID));
    commSync->commSend((long) terminalStrSz);
    commSync->commSend((long) level_num);
    commSync->commSend((long) partition_num);
    if (commSync->commRecv_byte() == SYNC_FAIL_FLAG)
        throw SyncFailureException("Sync parameters do not match.");
    Logger::gLog(Logger::COMM, "Sync parameters match");
}

void RCDS_Synchronizer::RecvSyncParam(const shared_ptr<Communicant> &commSync, bool oneWay) {
    Logger::gLog(Logger::METHOD, "Entering RCDS_Synchronizer::RecvSyncParam");
    // take care of parent sync method
//    SyncMethod::RecvSyncParam(commSync);

    // 也要一起取消
//    byte theSyncID = commSync->commRecv_byte();
    size_t TermStrSize_C = commSync->commRecv_long();
    size_t Levels_C = commSync->commRecv_long();
    size_t Partition_C = commSync->commRecv_long();

    // 暂时取消互传SyncID
//    if (theSyncID != enumToByte(SyncID) ||
    if (TermStrSize_C != terminalStrSz ||
        Levels_C != level_num ||
        Partition_C != partition_num) {
        // report a failure to establish sync parameters
        commSync->commSend(SYNC_FAIL_FLAG);
        Logger::gLog(Logger::COMM, "Sync parameters differ from client to server: Client has (" +
                                   to_string(TermStrSize_C) + "," + to_string(Levels_C) + "," + toStr(Partition_C) +
                                   ").  Server has (" + to_string(terminalStrSz) + "," + to_string(level_num) + "," +
                                   to_string(partition_num) + ").");
        throw SyncFailureException("Sync parameters do not match.");
    }
    commSync->commSend(SYNC_OK_FLAG);
    Logger::gLog(Logger::COMM, "Sync parameters match");
}

void RCDS_Synchronizer::configure(shared_ptr<SyncMethod> &setHost, long mbar, size_t elem_size) {
    if (GenSync::SyncProtocol::IBLTSyncSetDiff == baseSyncProtocol)
        setHost = make_shared<IBLTSync_SetDiff>(mbar, elem_size, true);
    else if (GenSync::SyncProtocol::InteractiveCPISync == baseSyncProtocol)
        // 按test方法改了参数
//        setHost = make_shared<InterCPISync>(5, elem_size * 8, 64, 3, true);
        setHost = make_shared<InterCPISync>(2 * UCHAR_MAX, pow((double) sizeof(randZZ()), 2.0), 8, 5, true);
    else if (GenSync::SyncProtocol::CPISync == baseSyncProtocol)
        // 按test方法改了参数
//        setHost = make_shared<ProbCPISync>(mbar, elem_size * 8, 64, true);
        setHost = make_shared<CPISync>(2 * UCHAR_MAX, elem_size * 8, 8, false);
}
