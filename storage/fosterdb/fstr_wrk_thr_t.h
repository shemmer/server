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


const int FOSTER_CREATE= 5;
const int FOSTER_DELETE= 6;

const int FOSTER_WRITE_ROW = 10;
const int FOSTER_DELETE_ROW=11;
const int FOSTER_UPDATE_ROW=12;


struct base_request_t{
    mysql_cond_t COND_work;
    mysql_mutex_t LOCK_work_mutex;

    bool notified=false;

    int type;
    int err;
};

struct start_stop_request_t : base_request_t{
};

struct ddl_request_t : base_request_t{
    String table_name;
    TABLE* table;

    uint max_key_name_len;
};

struct write_request_t : base_request_t{
    uchar* mysql_format_buf;

    uchar* old_mysql_format_buf;

    uchar* key_buf;

    String table_name;

    uchar* rec_buf;

    uint maxlength;


    TABLE* table;
    uint max_key_len;
};


class fstr_wrk_thr_t : public smthread_t{
    static ss_m* foster_handle;


    base_request_t* req;

    w_rc_t startup();
    w_rc_t shutdown();

    int work_ACTIVE();

    w_rc_t create_physical_table();
    w_rc_t delete_table();


    w_rc_t add_tuple();
    w_rc_t delete_tuple();
    w_rc_t update_tuple();


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
