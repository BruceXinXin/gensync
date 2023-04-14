//
// Created by bruce on 2/16/2023.
//

// Folder: (current implementation)
// get folder dests and put all names in a list with abs path
// go through each and ask the other side to sync with one of the modes:
// 1: no file name match - send the entire file over
// 2: file name match size not match - set of content sync
// 3: file name and size match - skip (we think they are the same)


#ifndef CPISYNC_RCDS_H
#define CPISYNC_RCDS_H

#include <CPISync/Aux_/Auxiliary.h>
#include <CPISync/Aux_/SyncMethod.h>
#include <CPISync/Communicants/Communicant.h>
#include <CPISync/Data/DataObject.h>
#include "GenSync.h"
#include "InterCPISync.h"
#include "ProbCPISync.h"
#include "RCDS_Synchronizer.h"

class RCDS : public SyncMethod {
public:
    // NOT_SET改为0
    // 后三个参数没用(都是在SetOfContent中的参数, RCDS中没用着), 待删除
//    RCDS(GenSync::SyncProtocol base_set_proto, size_t terminal_str_size = 10, size_t levels = NOT_SET,
//         size_t partition = NOT_SET);
//    RCDS(GenSync::SyncProtocol base_set_proto, size_t terminal_str_size = 10, size_t levels = 0,
//         size_t partition = 0);
    RCDS(GenSync::SyncProtocol RCDS_base_proto, shared_ptr<Communicant>& newComm, size_t terminal_str_size = 10, size_t levels = 0,
         size_t partition = 0);

    ~RCDS() = default;

    /**
     * Init a folder
     * @param str
     * @param datum
     * @param sync
     * @return
     */
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
    size_t m_termStrSize, m_levels, m_partition;
    string m_folder_name;
    vector<shared_ptr<DataObject>> m_filenames;
    map<size_t, string> m_hash2filename;
    bool m_save_file, m_single_file_mode;
    shared_ptr<Communicant> newCommunicant; // comm for baseSyncProtocol


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
