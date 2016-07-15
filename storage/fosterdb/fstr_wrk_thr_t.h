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
#include <memory>
#include <w.h>

typedef unsigned char uchar;
typedef unsigned int uint32;
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


typedef struct condex
{
    pthread_cond_t _cond;
    pthread_mutex_t _lock;
    long _signals;
    long _waits;


    condex() : _signals(0), _waits(0) {
        if (pthread_cond_init(&_cond,NULL)) {
            assert (0); // failed to init cond var
        }
        if (pthread_mutex_init(&_lock,NULL)) {
            assert (0); // failed to init mutex
        }
    }

    ~condex() {
        pthread_cond_destroy(&_cond);
        pthread_mutex_destroy(&_lock);
    }

    void signal() {
        CRITICAL_SECTION(cs, _lock);
        _signals++;
        pthread_cond_signal(&_cond);
    }

    void wait() {
        CRITICAL_SECTION(cs, _lock);
        _waits++;
        while(_waits > _signals)
            pthread_cond_wait(&_cond,&_lock);
    }

} foster_condex;


typedef struct st_foster_record_buffer {
    uchar *buffer;
    uint32 length;

    bool fix_rec_buff(unsigned int length){
        assert(buffer);
        if (this->length < length) {
            uchar *newptr;
            if (!(newptr = (uchar *) realloc(buffer, length))) {
                cerr<<" Cant create record_buffer"<<endl;
                return false;
            }
            buffer = newptr;
            this->length = length;
        }
        assert(buffer);
        assert(length<=this->length);
        return true;
    }

    st_foster_record_buffer(ulong length){
        if(!(buffer= (uchar*) malloc(length)))
        {
            cerr<<" Cant create record_buffer"<<endl;
            return;
        }
        this->length= (int)length;
    }
    ~st_foster_record_buffer(){
        free(buffer);
    }
} foster_record_buffer;

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
    foster_condex* request_condex;
    bool notified=false;

    int type;
    int err;

    base_request_t(int t):
            type(t), request_condex(new foster_condex()){

    }
};


struct discovery_request_t : base_request_t{
    w_keystr_t* cat_entry_key;
    kj::ArrayPtr<const capnp::word> table_info_array;

    discovery_request_t(int t):
            base_request_t(t){
    }
};

struct start_stop_request_t : base_request_t{
    char* db;
    char* logdir;
    start_stop_request_t(int t):
            base_request_t(t){
    }
};


struct ddl_request_t : base_request_t{
    string table_name;
    string db_name;
    FosterTableInfo::Builder capnpTable =nullptr;
    ::capnp::MallocMessageBuilder message;
    std::vector<FOSTER_FK_INFO> foreign_keys;
    ddl_request_t(int t):
            base_request_t(t){
    }

};

struct write_request_t : base_request_t{
    foster_record_buffer* packed_record_buf;
    uint packed_len;

    uchar* mysql_format_buf;

    uchar* old_mysql_format_buf;

    uchar* key_buf;


    uint maxlength;

    uint max_key_len;

    kj::ArrayPtr<const capnp::word> table_info_array;


    write_request_t(int t):
            base_request_t(t){
    }

};

struct read_request_t : base_request_t{
    search_mode find_flag;

    uchar* key_buf;
    uint ksz;

    foster_record_buffer* packed_record_buf;
    uint packed_len;

    int len;

    int idx_no;

    kj::ArrayPtr<const capnp::word> table_info_array;

    read_request_t(int t):
            base_request_t(t){
    }
};


inline void construct_cat_key(char* db_name, uint db_len,
                                    char* table_name, uint table_name_len,
                              w_keystr_t& in){
    char* temp = (char*) malloc(db_len+table_name_len+3);
    for(uint i=0; i<db_len; i++){
        temp[i]=db_name[i];
    }
    temp[db_len]= '#';
    temp[db_len+1]= '#';
    temp[db_len+2]= '#';
    for(uint i=0; i<table_name_len; i++){
        temp[db_len+3+i] = table_name[i];
    }
    in.construct_regularkey(temp,db_len+table_name_len+3);
    free(temp);
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

    base_request_t* req;

    shared_ptr<base_request_t> shared_req;

    w_rc_t startup();
    w_rc_t shutdown();

    int work_ACTIVE();

    w_rc_t discover_table(shared_ptr<discovery_request_t> r);

    w_rc_t create_physical_table(shared_ptr<ddl_request_t> r);
    w_rc_t delete_table(shared_ptr<ddl_request_t> r);

    w_rc_t add_tuple(shared_ptr<write_request_t> r);
    w_rc_t delete_tuple(shared_ptr<write_request_t> r);
    w_rc_t update_tuple(shared_ptr<write_request_t> r);

    w_rc_t index_probe(shared_ptr<read_request_t> r);
    w_rc_t next(shared_ptr<read_request_t> r);
    w_rc_t position_read(shared_ptr<read_request_t> r);

    void foster_config(sm_options* options);


    volatile bool notified;

    bool _exit=false;

    //TODO this is really ugly
    bool _find_exact=false;

    void add_to_secondary_idx(StoreID sec_id, w_keystr_t secondary, w_keystr_t primary);
    void delete_from_secondary_idx(StoreID sec_id, w_keystr_t sec_kstr, w_keystr_t primary);


    void add_to_secondary_idx_uniquified(StoreID sec_id, w_keystr_t secondary, w_keystr_t primary);
    void delete_from_secondary_idx_uniquified(StoreID sec_id, w_keystr_t sec_kstr, w_keystr_t primary);
public:
    int numUsedTables;
    bool aborted;
    bool commited;


    foster_condex* worker_condex;

    bt_cursor_t* cursor;

    fstr_wrk_thr_t();

    void run();

    void set_shared_request(shared_ptr<base_request_t> r) {
        shared_req= r;
    }

    inline void notify(bool notification){
        notified=notification;
    }
    void foster_exit();

    w_rc_t foster_begin();
    w_rc_t foster_commit();
    w_rc_t foster_rollback();

    std::function<int(int)> translate_err_code;


    string id;
};


#endif //MYSQL_FSTR_WRK_THR_H
