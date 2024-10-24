/*
 * Some useful definions and tools for work with YCSB
 */
#ifndef YCSB_READ_H
#define YCSB_READ_H

#include <string>
#include <cstring>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>

using namespace std;

/* entry parameters */
#ifndef _ENTRY_H_
#define _ENTRY_H_
#define KEY_LEN 8
#define VAL_LEN 8
#define KV_LEN (KEY_LEN+VAL_LEN)

struct Entry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};
#endif

#ifndef TEST
#define TEST
/* Vector stored load entries */
vector<Entry> entry;

enum command {
    QINSERT = 0,    // check if entry exists before insert
    INSERT = 1,     // insert command
    UPDATE = 2,     // update command
    READ = 3,       // query command
    DELETE = 4      // deletion command
};
map<string, command> str_to_cmd = { {"INSERT",INSERT}, {"UPDATE",UPDATE}, {"DELETE",DELETE}, {"READ",READ} };
/* Vector stored YCSB commands */
vector< tuple<command, uint64_t> > full_command;

vector<command> fc_cmd;
vector<Entry> fc_entry;
#endif

/* Read entries form a load dataset generated with YCSB */
void read_ycsb_load(string load_path)
{
    std::ifstream inputFile(load_path);

    if (!inputFile.is_open()) {
        std::cerr << "can't open file" << std::endl;
        return;
    }

    std::string line;
    int count = 0;

    while (std::getline(inputFile, line)) {
        // Look up the line with "usertable"
        size_t found = line.find("usertable user");
        if (found != std::string::npos) {
            // Get number follow "user"
            size_t userStart = found + strlen("usertable user");
            size_t userEnd = line.find(" ", userStart);

            if (userEnd != std::string::npos) {
                std::string userIDStr = line.substr(userStart, userEnd - userStart);
                try {
                    uint64_t userID = std::stoull(userIDStr);
                    entry.push_back({});
                    *(uint64_t*)entry[count++].key = userID;

                } catch (const std::invalid_argument& e) {
                    std::cerr << "invalid id: " << userIDStr << std::endl;
                } catch (const std::out_of_range& e) {
                    std::cerr << "id out of range: " << userIDStr << std::endl;
                }
            }
        }
    }

    cout<<"load: "<<count<<" keys"<<endl;
    
    inputFile.close();
}

/* Read commands form a run dataset generated with YCSB */
void read_ycsb_run(string run_path)
{
    std::ifstream inputFile(run_path);

    if (!inputFile.is_open()) {
        std::cerr << "can't open file" << std::endl;
        return;
    }

    std::vector<uint64_t> userIDs; // vector stored IDs each command line
    std::vector<std::string> prefixes; // vector stored prefixes each command line

    std::string line;
    int count = 0;

    while (std::getline(inputFile, line)) {
        // Look up the line with "usertable"
        size_t found = line.find("usertable user");
        if (found != std::string::npos) {

            // Get prefix in the front of "usertable"
            std::string prefix = line.substr(0, found-1);

            // Get number follow "user"
            size_t userStart = found + strlen("usertable user");
            size_t userEnd = line.find(" ", userStart);

            if (userEnd != std::string::npos) {
                std::string userIDStr = line.substr(userStart, userEnd - userStart);
                try {
                    uint64_t userID = std::stoull(userIDStr);

                    //full_command.emplace_back(tuple<command, uint64_t> (str_to_cmd[prefix], userID));

                    fc_cmd.push_back(str_to_cmd[prefix]);
                    fc_entry.push_back({});
                    *(uint64_t*)fc_entry[count++].key = userID;

                } catch (const std::invalid_argument& e) {
                    std::cerr << "invalid id: " << userIDStr << std::endl;
                } catch (const std::out_of_range& e) {
                    std::cerr << "id out of range: " << userIDStr << std::endl;
                }
            }
        }
    }

    cout<<"run: "<<count<<" keys"<<endl;
    
    inputFile.close();
}

#endif