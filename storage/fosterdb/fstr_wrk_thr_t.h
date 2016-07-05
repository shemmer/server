//
// Created by stefan on 16.03.16.
//

#ifndef MYSQL_FSTR_WRK_THR_H
#define MYSQL_FSTR_WRK_THR_H

#undef HAVE_SYS_MMAN_H
#undef HAVE_STDLIB_H
#undef HAVE_SYS_IOCTL_H
#undef HAVE_SYS_PARAM_H
#undef HAVE_SYS_STAT_H
#undef HAVE_SYS_TYPES_H
#undef HAVE_VPRINTF
#undef HAVE_MEMALIGN
#undef HAVE_VALLOC
#undef HAVE_STRERROR
#undef HAVE_CLOCK_GETTIME
#undef HAVE_GETTIMEOFDAY
#undef HAVE_PTHREAD_ATTR_GETSTACKSIZE
#undef HAVE_DIRENT_H
#undef HAVE_SEMAPHORE_H
#undef HAVE_STDINT_H
#undef VERSION
#undef HAVE_FCNTL_H
#undef HAVE_INTTYPES_H
#undef HAVE_MEMORY_H
#undef HAVE_NETINET_IN_H

#include <map>
#include <set>
#include <functional>

#include "sm_vas.h"

#include "cat_entries.capnp.h"
#include <capnp/message.h>
#include <capnp/serialize-packed.h>

typedef struct st_fstr_fk_info
{
    string foreign_id;
    string foreign_table;

    string referenced_db;
    string referenced_table;


    uint update_method=0;
    uint delete_method=0;


    std::vector<string> foreign_fields;
    std::vector<string> referenced_fields;

    kj::ArrayPtr<const capnp::word> reftables;
} FOSTER_FK_INFO;


const int FOSTER_STARTUP =1;
const int FOSTER_SHUTDOWN=2;

const int FOSTER_DISCOVERY=3;

const int FOSTER_CREATE= 5;
const int FOSTER_DELETE= 6;

const int FOSTER_WRITE_ROW = 10;
const int FOSTER_DELETE_ROW=11;
const int FOSTER_UPDATE_ROW=12;

const int FOSTER_IDX_READ = 15;
const int FOSTER_IDX_NEXT = 16;
const int FOSTER_POS_READ = 18;

const int FOSTER_COMMIT =20;
const int FOSTER_ROLLBACK=21;
const int FOSTER_BEGIN=22;

struct base_request_t{
    pthread_cond_t COND_work;
    pthread_mutex_t LOCK_work_mutex;

    bool notified=false;

    int type;
    int err;
};

typedef unsigned char uchar;

struct discovery_request_t : base_request_t{
    string db_name;
    string table_name;

    kj::ArrayPtr<const capnp::word> table_info_array;
};

struct start_stop_request_t : base_request_t{
    char* db;
    char* logdir;
};


struct ddl_request_t : base_request_t{
    string table_name;
    string db_name;

    FosterTableInfo::Builder capnpTable =nullptr;
    ::capnp::MallocMessageBuilder message;

    std::vector<FOSTER_FK_INFO> foreign_keys;
};

struct write_request_t : base_request_t{
    uchar* packed_record_buf;
    uint packed_len;

    uchar* mysql_format_buf;

    uchar* old_mysql_format_buf;

    uchar* key_buf;


    uint maxlength;

    uint max_key_len;

    kj::ArrayPtr<const capnp::word> table_info_array;

};

//TODO copied!
enum search_mode {
    KEY_EXACT,
            KEY_OR_NEXT,
            KEY_OR_PREV,
            AFTER_KEY,
            BEFORE_KEY,
            PREFIX,
            PREFIX_LAST,
            PREFIX_LAST_OR_PREV,
    UNKNOWN
};

struct read_request_t : base_request_t{
    search_mode find_flag;

    uchar* key_buf;
    uint ksz;

    uchar* packed_record_buf;
    uint packed_len;

    int len;

    int idx_no;

    kj::ArrayPtr<const capnp::word> table_info_array;
};

inline w_keystr_t construct_cat_key(string db_name, string table_name){
    string primaryStr;
    w_keystr_t ret;
    for(uint i=0; i<db_name.length(); i++){
        if(db_name.at(i)==' ') continue;
        primaryStr.push_back(db_name.at(i));
    }
    primaryStr.push_back('#');
    primaryStr.push_back('#');
    primaryStr.push_back('#');
    for(uint i=0; i<table_name.length(); i++){
        if(table_name.at(i)==' ') continue;
        primaryStr.push_back(table_name.at(i));
    }
    ret.construct_regularkey(primaryStr.c_str(),primaryStr.length());
    return ret;
}
inline void print_capnp_table(FosterTableInfo::Reader tableinfo){
    cerr<<"################TABLE###################"<<endl;
    cerr<< "Database: " << tableinfo.getDbname().cStr()<<endl;
    cerr<< "Table: "<< tableinfo.getTablename().cStr()<<endl;
    cerr<<"##################INDEXES################"<<endl;
    capnp::List<FosterIndexInfo>::Reader index_reader = tableinfo.getIndexes();
    for(uint i=0; i< index_reader.size(); i++){
        FosterIndexInfo::Reader curr_index_reader = index_reader[i];
        cerr<< "Index Name: " << curr_index_reader.getIndexname().cStr()<<endl;
        cerr<< "\tStorage ID: " <<curr_index_reader.getStid() <<endl;
        cerr<< "\tKeylength: " << curr_index_reader.getKeylength() <<endl;
        cerr<< "\tPrimary STID:  "<<curr_index_reader.getPrimary() <<endl;
        capnp::List<FosterFieldInfo>::Reader part_reader = curr_index_reader.getPartinfo();
        for(uint j =0; j<part_reader.size(); j++){
            FosterFieldInfo::Reader curr_field = part_reader[j];
            cerr<< "\t\tField Name: " << curr_field.getFieldname().cStr()<<endl;
            cerr<< "\t\tField Length: " << curr_field.getLength()<<endl;
            cerr<< "\t\tField Offset: " << curr_field.getOffset()<<endl;
            cerr<< "\t\tField Nullbit: " << curr_field.getNullBit()<<endl;
            cerr<< "\t\tField NullOffset: " << curr_field.getNullOffset()<<endl;
        }
    }
    cerr<<"##############FOREIGN RELATIONS#########"<<endl;
    capnp::List<FosterForeignTable>::Reader foreign_reader = tableinfo.getForeignkeys();
    for(uint i=0; i< foreign_reader.size(); i++){
        FosterForeignTable::Reader curr_foreign = foreign_reader[i];
        cerr<< "Foreign ID: " << curr_foreign.getId().cStr()<<endl;
        cerr<< "\tForeign Table ID: " << curr_foreign.getForeignTableId().cStr()<<endl;
        cerr<< "\tForeign Index ID: " << curr_foreign.getForeignIdx().cStr()<<endl;
        cerr<< "\tForeign Index Position: " << curr_foreign.getForeignIdxPos()<<endl;
        cerr<< "\tReferenced Index: " << curr_foreign.getReferencingIdx().cStr()<<endl;
        cerr<< "\tReferenced Index Position: " << curr_foreign.getReferencingIdxPos()<<endl;
    }
    cerr<<"#############REFERENCED RELATIONS########"<<endl;

    capnp::List<FosterReferencingTable>::Reader referencing_reader = tableinfo.getReferencingKeys();
    for(uint i=0; i<referencing_reader.size(); i++){
        FosterReferencingTable::Reader curr_referencing = referencing_reader[i];
        cerr<< "Referencing ID: " << curr_referencing.getId().cStr()<<endl;
        cerr<< "\tReferencing Table: " <<curr_referencing.getReferencingTable().cStr()<<endl;
        cerr<< "\tReferencing Index: " <<curr_referencing.getReferencingIdx().cStr()<< endl;;
        cerr<< "\tForeign Index ID: " << curr_referencing.getForeignIdx().cStr()<<endl;
        cerr<< "\tForeign Index Position: " << curr_referencing.getForeignIdxPos()<<endl;
//        cerr<< "Action" << curr_referencing.getAction() <<endl;
//        cerr<< "Type" << curr_referencing.getType()<<endl;
    }
    cerr<<"################TABLE_END##################"<<endl;
}

class bt_cursor_t;
class sm_options;
class fstr_wrk_thr_t : public smthread_t{
    static ss_m* foster_handle;

    bt_cursor_t* cursor;

    base_request_t* req;

    uint curr_numberOfRecs;
    uint curr_element;

    w_rc_t startup();
    w_rc_t shutdown();

    int work_ACTIVE();

    w_rc_t discover_table(discovery_request_t* r);

    w_rc_t create_physical_table(ddl_request_t* r);
    w_rc_t delete_table(ddl_request_t* r);

    w_rc_t add_tuple(write_request_t* r);
    w_rc_t delete_tuple(write_request_t* r);
    w_rc_t update_tuple(write_request_t* r);

    w_rc_t index_probe(read_request_t* r);
    w_rc_t next(read_request_t* r);
    w_rc_t position_read(read_request_t* r);

    void foster_config(sm_options* options);


    volatile bool notified;

    bool _exit=false;

    //TODO this is really ugly
    bool _find_exact=false;

    int add_to_secondary_idx(StoreID sec_id, w_keystr_t secondary, w_keystr_t primary);
    int delete_from_secondary_idx(StoreID sec_id, w_keystr_t sec_kstr, w_keystr_t primary);


    int add_to_secondary_idx_uniquified(StoreID sec_id, w_keystr_t secondary, w_keystr_t primary);
    int delete_from_secondary_idx_uniquified(StoreID sec_id, w_keystr_t sec_kstr, w_keystr_t primary);
public:

    pthread_mutex_t thread_mutex;
    int numUsedTables;
    bool aborted;

    pthread_cond_t COND_worker;

    fstr_wrk_thr_t();

    void run();
    void set_request(base_request_t* r) {
        req = r;
    }

    inline void notify(bool notification){
        notified=notification;
    }
    void foster_exit();

    w_rc_t foster_begin();
    w_rc_t foster_commit();
    w_rc_t foster_rollback();

    std::function<int(int)> translate_err_code;
};


typedef struct st_fstr_wrk_thr_pool{
    uint pool_size=10;
    std::vector<fstr_wrk_thr_t*> pool;
    pthread_mutex_t LOCK_pool_mutex;
    pthread_cond_t COND_pool;
    bool changed;
}FOSTER_THREAD_POOL;

#endif //MYSQL_FSTR_WRK_THR_H
