//
// Created by bruce on 2/16/2023.
//

#include <cassert>
#include <unordered_map>

#include <CPISync/Syncs/RCDS.h>

RCDS::RCDS(GenSync::SyncProtocol RCDS_base_proto, shared_ptr<Communicant>& newComm, size_t terminal_str_size, size_t levels, size_t partition)
        : m_RCDS_base_proto(RCDS_base_proto), newCommunicant(newComm), m_termStrSize(terminal_str_size), m_levels(levels), m_partition(partition) {

    // 多文件模式下不会调用Client的addStr, 也就不会更新这个标志, 先置位单文件模式为false
    m_single_file_mode = false;
    m_save_file = true;
}

// 把文件名字符串以DataObject形式放入setPointers(同时更新了FolderName), 并判断是否是singleFileMode
// datum 和 sync 没用, 考虑删去
void RCDS::addStr(shared_ptr<DataObject>& str) {
    // 不允许多个文件夹
    string tmp = str->to_string();
    if (!m_folder_name.empty()) {
        if (m_folder_name != tmp)
            Logger::error_and_quit("Syncing multiple folders is not supported!");
    }
    else
        m_folder_name = tmp;
    if (m_folder_name.empty())
        Logger::error_and_quit("Folder name cannot be empty!");

    if (!isFile((m_folder_name))) {
        while (!m_folder_name.empty() && m_folder_name.back() == '/')
            m_folder_name.pop_back();
        for (const string &f_name : walkRelDir(m_folder_name))
            m_filenames.push_back(make_shared<DataObject>(f_name));
        m_single_file_mode = false;
    } else {
        m_filenames.push_back(make_shared<DataObject>(m_folder_name));
        m_single_file_mode = true;
    }
}

bool RCDS::addElem(shared_ptr<DataObject> newDatum) {
    Logger::gLog(Logger::METHOD,"Entering RCDS::addElem");

    if(!SyncMethod::addElem(newDatum)) return false;
    addStr(newDatum);
    Logger::gLog(Logger::METHOD, "Successfully added shared_ptr<DataObject> {" + newDatum->print() + "}");
    return true;
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
        int levels = floor(log10(getFileSize(m_folder_name)));
        int par = 4;
        commSync->commSend(levels);
        string_server(commSync, m_folder_name, levels, par);
    } else {
        if (m_folder_name.back() != '/')
            m_folder_name += "/";

        vector<shared_ptr<DataObject>> unique_set = check_and_get(m_filenames);
        send_diff_files(commSync, 10e2, sizeof(size_t), unique_set, selfMinusOther, otherMinusSelf);
//        newCommunicant->commClose();

        // 清空otherMinusSelf
        otherMinusSelf.clear();

        commSync->commSend((long) selfMinusOther.size());
        for (auto &f: selfMinusOther) {
            string filename = hash2filename(f->to_ZZ());

            commSync->commSend((filename.empty()) ? "E" : filename);
            if (filename.empty())
                continue;

            string full_filename = m_folder_name + filename;
            int mode = 0;
            // 接收OK/NO_INFO
            (commSync->commRecv_byte() == SYNC_OK_FLAG) ? mode = 1 : mode = 2;

            // 这里发的FAIL临时指代小文件发送方式
            if (mode == 1 and getFileSize(full_filename) < 500) {
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
                int levels = nearbyint(log10(getFileSize(full_filename)));
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
        string syncContent = string_client(commSync, m_folder_name, levels, par);
        if (m_save_file)
            writeStrToFile(m_folder_name, syncContent);
    } else {
        if (m_folder_name.back() != '/')
            m_folder_name += "/";

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
            string full_filename = m_folder_name + filename;
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

bool RCDS::string_server(const shared_ptr<Communicant> &commSync, const string& filename, int level, int partition) {
    // 必须要让useExisting为true!
    auto stringHost = RCDS_Synchronizer(m_RCDS_base_proto, level, partition);

    {
        string content = scanTxtFromFile(filename, numeric_limits<int>::max());
        auto str = make_shared<DataObject>(content);
        stringHost.add_str(str);
    }

    stringHost.SyncServer(commSync);
    return true;
}

string RCDS::string_client(const shared_ptr<Communicant> &commSync, const string& filename, int level, int partition) {
    // 必须要让useExisting为true!
    auto stringHost = RCDS_Synchronizer(m_RCDS_base_proto, level, partition);

    list<shared_ptr<DataObject>> selfMinusOther, otherMinusSelf;
    string content = scanTxtFromFile(filename, numeric_limits<int>::max());

    shared_ptr<DataObject> res, str = make_shared<DataObject>(content);
    try {
        stringHost.add_str(str);
    } catch (std::exception &e) {
        // ignore exception
        cout << e.what() << endl;
    }
    stringHost.SyncClient(commSync);
    // 重建字符串
    stringHost.recover_str(res);
    return res->to_string();
}