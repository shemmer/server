//
// Created by stefan on 16.03.16.
//

#include <key.h>
#include <my_base.h>

#include "fstr_wrk_thr_t.h"


#include "sm_vas.h"
#include "btcursor.h"



ss_m* fstr_wrk_thr_t::foster_handle;

fstr_wrk_thr_t::fstr_wrk_thr_t(bool begin):
        _begin_tx(begin), smthread_t(t_regular, "trx_worker"){
    init_foster_psi_keys();
    mysql_mutex_init(key_mutex_foster_wrker,
                     &thread_mutex, MY_MUTEX_INIT_FAST);
    mysql_cond_init(key_COND_worker, &COND_worker, NULL);
    notified = false;
    this->fork();
}



void fstr_wrk_thr_t::foster_exit(){
    mysql_mutex_lock(&thread_mutex);
    notify(true);
    _exit=true;
    mysql_cond_signal(&COND_worker);
    mysql_mutex_unlock(&thread_mutex);
    join();
    mysql_mutex_destroy(&thread_mutex);
}


static void foster_config(sm_options* options){

    string logdir ="/home/stefan/mariadb/zero_log/log";
    string archdir;
    string opt_dbfile = "/home/stefan/mariadb/zero_log/db";
    string opt_backup;

    bool format = false;

    string errlog ="/home/stefan/mariadb/zero_log/shoremt.err.log";
/**
 * REQUIRED
 */
    options->set_bool_option("sm_logging",true);
    options->set_bool_option("sm_archive_eager",false);

    options->set_string_option("sm_logdir", logdir);
    options->set_int_option("sm_bufpoolsize",1024);
    options->set_int_option("sm_logsize",8192);
    //TODO db file
    options->set_string_option("sm_dbfile", opt_dbfile);
    options->set_bool_option("sm_restart_instant",true);
    options->set_bool_option("sm_ticker_enable", false);

    options->set_bool_option("sm_truncate_log",false);
/**
 * Optional
 */
    options->set_int_option("sm_logbufsize",1024);
    options->set_bool_option("sm_reformat_log", false);
    options->set_string_option("sm_errlog", errlog); // - prints errlog to stderr
    options->set_string_option("sm_errlog_level","debug");
    options->set_int_option("sm_locktablesize", 64000);
    options->set_bool_option("sm_backgroundflush", true);
    options->set_string_option("sm_bufferpool_replacement_policy", "clock");
    options->set_bool_option("sm_bufferpool_swizzle", false);
    options->set_int_option("sm_num_page_writer", 1);
    options->set_bool_option("sm_prefetch", false);
    options->set_string_option("sm_backup_dir", ".");
    options->set_string_option("sm_archdir", "/home/stefan/mariadb/zero_log/arch");
//  options->set_bool_option("sm_archiving", true);
    options->set_bool_option("sm_async_merging", false);
    options->set_bool_option("sm_sort_archive", true);
    options->set_int_option("sm_merge_factor", 100);
    options->set_int_option("sm_merge_blocksize", 1048576);
//    options->set_int_option("sm_archiver_workspace_archive", 1);

    options->set_int_option("sm_archiver_bucket_size", 128);
    options->set_int_option("sm_archiver_block_size", 1048576);
    options->set_int_option("sm_fakeiodelay", 0);
    options->set_int_option("sm_fakeiodelay-enable", 0);

    options->set_int_option("sm_log_fetch_buf_partitions", 0);

    options->set_int_option("sm_log_page_flushers", 1);

    options->set_int_option("sm_preventive_chkpt", 0);

    options->set_int_option("sm_chkpt_interval", 10000);

    ifstream dbFile(opt_dbfile.c_str());
    if(!dbFile){
        format=true;
        FILE *f = fopen(opt_dbfile.c_str(),"w");
    }

    options->set_bool_option("sm_format",format);
    ifstream errFile(errlog.c_str());
    if(!errlog.compare("-"))
    if(!errFile) FILE *e = fopen(errlog.c_str(),"w");
}


void fstr_wrk_thr_t::run(){
    DBUG_ENTER("fstr_wrk_thr::run");
    int rval=0;
    while(true){
        mysql_mutex_lock(&thread_mutex);
        while(!notified){
            mysql_cond_wait(&COND_worker, &thread_mutex);
        }
        mysql_mutex_unlock(&thread_mutex);
        notified=false;
        if(_exit) DBUG_VOID_RETURN;
        if(_begin_tx){
            W_COERCE(foster_handle->begin_xct());
            _begin_tx=false;
        }
        rval = work_ACTIVE();
        if(rval) DBUG_VOID_RETURN;
    }
}


int fstr_wrk_thr_t::work_ACTIVE(){
    DBUG_ENTER("fstr_wrk_thr::work_ACTIVE");
    w_rc_t err;
    switch(req->type){
        case FOSTER_STARTUP:
            err=startup();break;
        case FOSTER_SHUTDOWN:
            err=shutdown();break;
        case FOSTER_CREATE:
            err=create_physical_table();break;
        case FOSTER_DELETE:
            err=delete_table(); break;
        default: break;
    }
    if(err.is_error()) {
        switch (err.err_num()) {
            case eDUPLICATE:
                req->err = HA_ERR_FOUND_DUPP_KEY;break;
            default:
                req->err = err.err_num();break;
        }
    }else{
        req->err=0;
    }
    req->notified=true;
    mysql_cond_signal(&req->COND_work);
    DBUG_RETURN(0);
}

w_rc_t fstr_wrk_thr_t::shutdown() {
    DBUG_ENTER("fstr_wrk_thr::shutdown");
    delete(foster_handle);
    DBUG_RETURN(RCOK);
}

w_rc_t fstr_wrk_thr_t::startup() {
    DBUG_ENTER("fstr_wrk_thr::startup");
    start_stop_request_t *start_req = static_cast<start_stop_request_t *>(req);
    sm_options* options = new sm_options();
    foster_config(options);
    foster_handle = new ss_m(*options);
    vol_t *vol = ss_m::vol;

    w_assert0(vol);
    if (!vol->is_alloc_store(1))
    {
        // create catalog index (must be on stid 1) if its not allocated create a new one
        StoreID cat_stid =1;

        W_COERCE(foster_handle->begin_xct());
        W_COERCE(foster_handle->create_index(cat_stid));
        w_assert0(cat_stid == 1);
        W_COERCE(foster_handle->commit_xct());
//        ss_m::checkpoint();
    }else{
        StoreID cat_stid;
    }
    DBUG_RETURN(RCOK);
}


w_rc_t fstr_wrk_thr_t::create_physical_table(){
    DBUG_ENTER("fstr_wrk_thr::create_physical_table");
    ddl_request_t* r = static_cast<ddl_request_t*>(req);
    // Add entry on catalog
    StoreID cat_stid =1;
    //Fill an idx_name buffer with tablename and idx name
    uchar separator[4]={"###"};

    uchar* idx_name_buf= (uchar*) my_malloc(r->table_name.length() + r->max_key_name_len + 3, MYF(MY_WME));
    for(int i=0; i<r->table->s->keys; i++){

        memcpy(idx_name_buf, r->table_name.ptr(), r->table_name.length());

        idx_name_buf +=r->table_name.length();
        memcpy(idx_name_buf, separator, 3);
        idx_name_buf+=3;

        memcpy(idx_name_buf, r->table->key_info[i].name, r->table->key_info[i].name_length);
        idx_name_buf -= (r->table_name.length()+3);
        StoreID stid_tmp;
        //Create new index
        W_COERCE(foster_handle->create_index(stid_tmp));
        //Construct a key
        w_keystr_t kstr;
        kstr.construct_regularkey(idx_name_buf,r->table_name.length()+r->table->key_info[i].name_length+3);
        //Create assoc in catalog
        W_COERCE(foster_handle->create_assoc(cat_stid, kstr, vec_t(&stid_tmp, sizeof(StoreID))));
    }
    W_COERCE(foster_handle->commit_xct());
    DBUG_RETURN(RCOK);
}


w_rc_t fstr_wrk_thr_t::delete_table() {
    ddl_request_t* r = static_cast<ddl_request_t*>(req);
    StoreID cat_stid =1;
    uchar separator[4]={"###"};
    w_keystr_t table_keystr;
    table_keystr.construct_regularkey(r->table_name.ptr(), r->table_name.length());
    w_keystr_t infimum, supremum;
    infimum.construct_posinfkey();
    supremum.construct_neginfkey();
    bt_cursor_t* catalog_cursor = new bt_cursor_t(cat_stid, supremum, false, infimum, false, true);
    catalog_cursor->next();
    w_keystr_t curr;
    do{
        curr= catalog_cursor->key();
        basic_string<unsigned char> data= curr.serialize_as_nonkeystr();
        basic_string<unsigned char> separator_str =basic_string<unsigned char> (separator, 3);

        //Drop every catalog entry with the same table name
        if(data.compare(0,r->table_name.length()-1,table_keystr.serialize_as_nonkeystr(), 0,r->table_name.length()-1)){
            if(data.compare(r->table_name.length(),3,separator_str,0,3)){
                //foster_handle->destroy_assoc(cat_stid,curr);
            }
        }
        catalog_cursor->next();
    }while(!catalog_cursor->eof());
    return RCOK;
}
