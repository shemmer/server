//
// Created by stefan on 16.03.16.
//

#ifndef MYSQL_FSTR_WRK_THR_H
#define MYSQL_FSTR_WRK_THR_H

#include <w_key.h>
#include "my_global.h"
#include "sql_class.h"

#include "smthread.h"


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

struct base_request_t{
    mysql_cond_t COND_work;
    mysql_mutex_t LOCK_work_mutex;

    bool notified=false;

    int type;
    int err;
};


struct discovery_request_t : base_request_t{
    char* idx_name_buf;
    size_t idx_name_buf_len;
    uint* stid;
};

struct start_stop_request_t : base_request_t{
    char* db;
    char* logdir;
};



struct ddl_request_t : base_request_t{
    String table_name;
    TABLE* table;

    uint max_key_name_len;
};

struct write_request_t : base_request_t{
    std::vector<uint> stids;

    uchar* mysql_format_buf;

    uchar* old_mysql_format_buf;

    uchar* key_buf;

    String table_name;

    uchar* rec_buf;

    uint maxlength;

    TABLE* table;
    uint max_key_len;
};


struct read_request_t : base_request_t{
    uint stid;
    ha_rkey_function find_flag;

    uchar* key_buf;
    uint ksz;

    uchar* mysql_format_buf;

    String table_name;
    int len;

    int idx_no;
    TABLE* table;

    uint pkstid;
};


class bt_cursor_t;
class sm_options;
class fstr_wrk_thr_t : public smthread_t{
    static ss_m* foster_handle;

    bt_cursor_t* cursor;

    base_request_t* req;

    uint sec_idx_offset;

    w_rc_t startup();
    w_rc_t shutdown();

    int work_ACTIVE();

    w_rc_t discover_table();

    w_rc_t create_physical_table();
    w_rc_t delete_table();

    w_rc_t add_tuple();
    w_rc_t delete_tuple();
    w_rc_t update_tuple();

    w_rc_t index_probe();
    w_rc_t next();
    w_rc_t position_read();

    w_rc_t foster_commit();
    w_rc_t foster_rollback();

    void foster_config(sm_options* options);


#ifdef HAVE_PSI_INTERFACE
    PSI_mutex_key key_mutex_foster_wrker;
    PSI_mutex_info foster_worker_mutexes[1]=
            {
                    { &key_mutex_foster_wrker, "foster_worker::mutex", 0}
            };
    PSI_cond_key key_COND_worker;
    PSI_cond_info all_fstr_wrkr_cond[1]=
            {
                    { &key_COND_worker, "COND_worker", 0}
            };

    void init_foster_psi_keys()
    {
        const char* category= "foster-worker";
        int count;

        count= array_elements(foster_worker_mutexes);
        mysql_mutex_register(category, foster_worker_mutexes, count);
        count= array_elements(all_fstr_wrkr_cond);
        mysql_cond_register(category, all_fstr_wrkr_cond, count);
    }
#endif

    volatile bool notified;

    bool _exit=false;

    bool _begin_tx;

    int extract_key(uchar *key, int key_num, const uchar *record, TABLE* table);

    int pack_row(unsigned char *from, TABLE* table, unsigned char* buffer);

    int unpack_row(uchar *record, int row_len, uchar *&to, TABLE* table);

    int add_to_secondary_idx(StoreID sec_id, w_keystr_t secondary, w_keystr_t primary);
    int delete_from_secondary_idx(StoreID sec_id, w_keystr_t sec_kstr, w_keystr_t primary);
public:

    mysql_mutex_t thread_mutex;
    int numUsedTables;

    mysql_cond_t COND_worker;

    fstr_wrk_thr_t(bool begin);

    void run();
    void set_request(base_request_t* r) {
        req = r;
    }

    inline void notify(bool notification){
        notified=notification;
    }
    void foster_exit();

};


#endif //MYSQL_FSTR_WRK_THR_H
