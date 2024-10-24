#include "SmoothExpansionFramework.h"
#include "race.h"
#include "CuckooDuoSIGLEN.h"
#pragma once

// race_dict = Dictionary dict([]() -> BaseSegment* {
//     return new RaceSegment();
// });
class RaceSegment: public SEF::BaseSegment{
public:
SEF::Entry all_inserted_entry_list[MAX_ITEMS_PER_SEGMENT];
int total_insert_counter;
RACE::RACETable *race;

    RaceSegment(){
        memset(all_inserted_entry_list, 0, sizeof(all_inserted_entry_list));
        total_insert_counter = 0;
        race = new RACE::RACETable(MAX_ITEMS_PER_SEGMENT);
    }
    ~RaceSegment(){
        delete race;
    }

    bool insert(const SEF::Entry& entry) override {
        RACE::Entry _entry;
        memcpy(_entry.key, entry.key, sizeof(entry.key));
        memcpy(_entry.val, entry.val, sizeof(entry.val));
        all_inserted_entry_list[total_insert_counter++] = entry;
        if (total_insert_counter >= MAX_ITEMS_PER_SEGMENT) return false;
        bool result = race->insert(_entry);
        return result;
    }

    int dump(SEF::Entry *entry) override {
        memcpy(entry, all_inserted_entry_list, sizeof(all_inserted_entry_list));
        return total_insert_counter;
    }

    void clean() override {
        memset(all_inserted_entry_list, 0, sizeof(all_inserted_entry_list));
        total_insert_counter = 0;
        delete race;
        race = new RACE::RACETable(MAX_ITEMS_PER_SEGMENT);
    }

    int memory() override {
        // RACE ignore cpu memory usage experiment, only LF
        return MAX_ITEMS_PER_SEGMENT;
    }

    int memory_per_item() override {
        return 1;
    }

    int current_items() override {
        return total_insert_counter;
    }
};




// cd_dict = Dictionary dict([]() -> BaseSegment* {
//     return new CuckooDuoSegment();
// });
class CuckooDuoSegment: public SEF::BaseSegment{
public:
SEF::Entry all_inserted_entry_list[MAX_ITEMS_PER_SEGMENT];
int total_insert_counter;
CK::CuckooHashTable *cuckoo;

    CuckooDuoSegment(){
        memset(all_inserted_entry_list, 0, sizeof(all_inserted_entry_list));
        total_insert_counter = 0;
        cuckoo = new CK::CuckooHashTable(MAX_ITEMS_PER_SEGMENT, 10);
    }
    ~CuckooDuoSegment(){
        delete cuckoo;
    }

    bool insert(const SEF::Entry& entry) override {
        CK::inputEntry _entry;
        memcpy(_entry.key, entry.key, sizeof(entry.key));
        memcpy(_entry.val, entry.val, sizeof(entry.val));
        all_inserted_entry_list[total_insert_counter++] = entry;
        if (total_insert_counter >= MAX_ITEMS_PER_SEGMENT) return false;
        bool result = cuckoo->insert(_entry);
        return result;
    }

    int dump(SEF::Entry *entry) override {
        memcpy(entry, all_inserted_entry_list, sizeof(all_inserted_entry_list));
        return total_insert_counter;
    }

    void clean() override {
        memset(all_inserted_entry_list, 0, sizeof(all_inserted_entry_list));
        total_insert_counter = 0;
        delete cuckoo;
        cuckoo = new CK::CuckooHashTable(MAX_ITEMS_PER_SEGMENT, 10);
    }

    int memory() override {
        return MAX_ITEMS_PER_SEGMENT * memory_per_item();
    }

    int memory_per_item() override {
        return SIG_BIT;
    }

    int current_items() override {
        return total_insert_counter;
    }
};