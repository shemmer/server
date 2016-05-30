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


void fstr_wrk_thr_t::foster_config(sm_options* options){

    start_stop_request_t *start_req = static_cast<start_stop_request_t *>(req);

    string logdir(start_req->logdir);
    string opt_dbfile(start_req->db);

    string archdir;
    string opt_backup;

    bool format = false;

    string errlog ="/home/stefan/mariadb/zero_log/shoremt.err.log";
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
            case FOSTER_DISCOVERY:
            err=discover_table(); break;
        case FOSTER_CREATE:
            err=create_physical_table();break;
        case FOSTER_DELETE:
            err=delete_table(); break;
        case FOSTER_WRITE_ROW:
            err=add_tuple(); break;
        case FOSTER_UPDATE_ROW:
            update_tuple(); break;
        case FOSTER_DELETE_ROW:
            err=delete_tuple(); break;
        case FOSTER_IDX_READ:
            err=index_probe(); break;
        case FOSTER_IDX_NEXT:
            err=next(); break;
        case FOSTER_POS_READ:
            err=position_read(); break;
        case FOSTER_COMMIT:
            foster_commit(); break;
        case FOSTER_ROLLBACK:
            err=foster_rollback();break;
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
    }
    DBUG_RETURN(RCOK);
}



w_rc_t fstr_wrk_thr_t::discover_table()
{
    discovery_request_t* r = static_cast<discovery_request_t*>(req);
    w_keystr_t kstr;
    kstr.construct_regularkey(r->idx_name_buf, r->idx_name_buf_len);
    StoreID stid;
    StoreID cat_stid =1;
    smsize_t size = sizeof(StoreID);
    bool found;
    W_COERCE(ss_m::find_assoc(cat_stid, kstr, &stid, size, found));
    r->stid=&stid;
}

/**
 * Extract a key with "key_num" from a record into "key"
 */
int fstr_wrk_thr_t::extract_key(uchar *key, int key_num, const uchar *record, TABLE* table)
{
    key_copy(key, const_cast<uchar*>(record), &table->key_info[key_num], 0);
    return  table->key_info[key_num].key_length;
}
/**
 * Memory -> Disk
 */
int fstr_wrk_thr_t::pack_row(uchar *from, TABLE* table, uchar* buffer)
{
    uchar* ptr;
    memcpy(buffer, from, table->s->null_bytes);
    ptr= buffer + table->s->null_bytes;

    for (Field **field=table->field ; *field ; field++)
    {
        if (!((*field)->is_null()))
            ptr= (*field)->pack(ptr, from + (*field)->offset(from));
    }
    return (unsigned int) (ptr - buffer);
}
/**
 * Disk->Memory
 */
int fstr_wrk_thr_t::unpack_row(uchar *record, int row_len, uchar *&to, TABLE* table)
{
    /* Copy null bits */
    const uchar* end= record+ row_len;
    memcpy(to, record, table->s->null_bytes);
    record+=table->s->null_bytes;
    if (record > end)
        return HA_ERR_WRONG_IN_RECORD;
    for (Field **field=table->field ; *field ; field++)
    {
        if (!((*field)->is_null_in_record(to)))
        {
            if (!(record= const_cast<uchar*>((*field)
                    ->unpack(to + (*field)->offset(table->record[0]),record, end))))
                return HA_ERR_WRONG_IN_RECORD;
        }
    }
    if (record != end)
        return HA_ERR_WRONG_IN_RECORD;
    return 0;
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



w_rc_t fstr_wrk_thr_t::add_tuple(){
    w_rc_t rc = RCOK;
    write_request_t* r = static_cast<write_request_t*>(req);
    // build key dat
    w_keystr_t kstr;
    //Construct primary key of tuple in _key_buf
    int ksz = extract_key(r->key_buf, 0, r->mysql_format_buf, r->table);
    kstr.construct_regularkey(r->key_buf, ksz);
    //Load stid of the primary idx
    StoreID pkstid = r->stids.at(0);
    uint rec_buf_len =(uint) pack_row(r->mysql_format_buf, r->table, r->rec_buf);
    //create assoc in primary index
    rc=foster_handle->create_assoc(pkstid, kstr, vec_t(r->rec_buf, rec_buf_len));
    if(rc.is_error()) { return rc;}

    if(r->stids.size()>1){
        uchar* sec_key_buffer = (uchar *) my_malloc(r->max_key_len, MYF(MY_WME));
        for (int idx = 1; idx < r->stids.size(); idx++) {
            StoreID sec_idx_stid = r->stids.at(idx);
            int sec_ksz = extract_key(sec_key_buffer, idx, r->mysql_format_buf, r->table);
            w_keystr_t sec_kstr;
            sec_kstr.construct_regularkey(sec_key_buffer, sec_ksz);
            add_to_secondary_idx(sec_idx_stid,sec_kstr,kstr);
        }
    }
    return (rc);
}

w_rc_t fstr_wrk_thr_t::update_tuple(){
    bool changed_pk=false;
    write_request_t* r = static_cast<write_request_t*>(req);
    StoreID pk_stid = r->stids.at(0);
    w_keystr_t new_kstr;
    int ksz = extract_key(r->key_buf, 0, r->mysql_format_buf, r->table);
    new_kstr.construct_regularkey(r->key_buf, ksz);
    uint rec_buf_len =(uint) pack_row(r->mysql_format_buf, r->table, r->rec_buf);
    W_DO(foster_handle->put_assoc(pk_stid,new_kstr,vec_t(r->rec_buf, rec_buf_len)));

    uchar* old_key_buffer;
    old_key_buffer = (uchar*) my_malloc(r->table->s->key_info[0].key_part->length, MYF(MY_WME));
    uint key_offset= r->table->s->key_info[0].key_part->offset;
    memcpy(old_key_buffer, r->old_mysql_format_buf+key_offset, r->table->s->key_info[0].key_part->length);

    //Build old key data of primary key
    w_keystr_t old_kstr;
    old_kstr.construct_regularkey(old_key_buffer, r->table->s->key_info[0].key_part->length);

    if(old_kstr.compare(new_kstr)!=0){
        W_DO(foster_handle->destroy_assoc(pk_stid, old_kstr));
        changed_pk=true;
    }

    if(r->stids.size()>1){
        uchar* sec_key_buffer = (uchar *) my_malloc(r->max_key_len, MYF(MY_WME));
        uchar* old_sec_key_buffer;
        for (int idx = 1; idx < r->stids.size(); idx++) {
            StoreID sec_idx_stid = r->stids.at(idx);

            int sec_ksz = extract_key(sec_key_buffer, idx, r->mysql_format_buf, r->table);

            w_keystr_t sec_kstr, old_sec_kstr;

            old_sec_key_buffer = (uchar*) my_malloc(r->table->s->key_info[idx].key_part->length, MYF(MY_WME));
            uint key_offset= r->table->s->key_info[0].key_part->offset;
            memcpy(old_key_buffer, r->old_mysql_format_buf+key_offset, r->table->s->key_info[0].key_part->length);
            old_sec_kstr.construct_regularkey(old_sec_key_buffer, r->table->s->key_info[idx].key_part->length);
            sec_kstr.construct_regularkey(sec_key_buffer, sec_ksz);
            add_to_secondary_idx(sec_idx_stid,sec_kstr,new_kstr);
            if(changed_pk) delete_from_secondary_idx(sec_idx_stid,sec_kstr, old_kstr);
        }
    }
    return RCOK;
}

w_rc_t fstr_wrk_thr_t::delete_tuple(){
    w_rc_t rc = RCOK;
    write_request_t* r = static_cast<write_request_t*>(req);
    // build key dat
    w_keystr_t kstr;
    //Construct primary key of tuple in _key_buf
    int ksz = extract_key(r->key_buf, 0, r->mysql_format_buf, r->table);
    kstr.construct_regularkey(r->key_buf, ksz);
    //Load stid of the primary idx
    StoreID pk_stid = r->stids.at(0);
    //create assoc in primary index
    rc=ss_m::destroy_assoc(pk_stid,kstr);
    if(r->stids.size()>1){
        uchar* sec_key_buffer = (uchar *) my_malloc(r->max_key_len, MYF(MY_WME));
        for (int idx = 1; idx < r->stids.size(); idx++) {
            StoreID sec_idx_stid = r->stids.at(idx);
            int sec_ksz = extract_key(sec_key_buffer, idx, r->mysql_format_buf, r->table);
            w_keystr_t sec_kstr;
            sec_kstr.construct_regularkey(sec_key_buffer, sec_ksz);
            delete_from_secondary_idx(sec_idx_stid,sec_kstr, kstr);
        }
    }
    return (rc);
}



w_rc_t fstr_wrk_thr_t::index_probe(){
    read_request_t* r = static_cast<read_request_t*>(req);
    //Infimum and supremum for use in cursors
    w_keystr_t infimum, supremum;
    infimum.construct_posinfkey();
    supremum.construct_neginfkey();
    // Build key data
    w_keystr_t kstr;

    bool partial =false;
    //Construct key of tuple in _key_b
    if(r->ksz!=0){
        kstr.construct_regularkey(r->key_buf, r->ksz);
        if(r->ksz <r->table->key_info[r->idx_no].key_length) {
            partial = true;
            //Null bit set
            if (r->key_buf[0] != 0) kstr.construct_neginfkey();
        }
    }else{
        r->find_flag==HA_READ_PREFIX_LAST ? kstr.construct_posinfkey() : kstr.construct_neginfkey();
    }

    StoreID idx_stid = r->stid;
    //Switch on search modes
    switch (r->find_flag) {
        case HA_READ_KEY_OR_NEXT:
            cursor = new bt_cursor_t(idx_stid, kstr, true, infimum, false, true);
            W_COERCE(cursor->next());
            if (cursor->eof()) {
                return RC(se_TUPLE_NOT_FOUND);
            }
            break;
        case HA_READ_KEY_OR_PREV:
            cursor = new bt_cursor_t(idx_stid, supremum, true, kstr, false, false);
            W_COERCE(cursor->next());
            if (cursor->eof()) {
                return RC(se_TUPLE_NOT_FOUND);
            }
            break;
        case HA_READ_KEY_EXACT: {
            cursor = new bt_cursor_t(idx_stid, kstr, true, infimum, false, true);
            W_COERCE(cursor->next());
            int comp = kstr.compare(cursor->key());
            if (comp < 0) {
                if(!partial) return RC(se_TUPLE_NOT_FOUND);
            }
        }
            break;
        case HA_READ_AFTER_KEY:
            cursor = new bt_cursor_t(idx_stid, kstr, false, infimum, false, true);
            W_COERCE(cursor->next());
            if (cursor->eof()) {
                return RC(se_TUPLE_NOT_FOUND);
            }
            break;
        case HA_READ_BEFORE_KEY:
            cursor = new bt_cursor_t(idx_stid, supremum, false, kstr, false, false);
            W_COERCE(cursor->next());
            if (cursor->eof()) {
                return RC(se_TUPLE_NOT_FOUND);
            }
            break;
        default:
            return (RCOK);
    }

    if(r->idx_no==0) {
        unpack_row((uchar *) cursor->elem(), cursor->elen(), r->mysql_format_buf, r->table);
    }else{
        bool found;
        uint pksz = r->table->key_info[0].key_length;

        curr_numberOfRecs =cursor->elem()[0];
        curr_element=0;

        uchar* tmp_pk_buffer = (uchar*) my_malloc((size_t) pksz, MYF(MY_WME));
        memcpy(tmp_pk_buffer, cursor->elem()+curr_element*pksz+1, pksz);
        w_keystr_t kstr;
        kstr.construct_regularkey(tmp_pk_buffer, pksz);
        //TODO can we get this better?
        smsize_t size=1;
        uchar* buf= (uchar*) my_malloc(size, MYF(MY_WME));
        w_rc_t rc = foster_handle->find_assoc(r->pkstid,kstr, buf, size, found);
        if(rc.is_error() && rc.err_num()==eRECWONTFIT){
            buf= (uchar*) my_realloc(buf, size, MYF(MY_WME));
            foster_handle->find_assoc(r->pkstid, kstr, buf, size,found);
        }
        unpack_row(buf, size, r->mysql_format_buf, r->table);
    }
    return (RCOK);

}

w_rc_t fstr_wrk_thr_t::next(){
    read_request_t* r = static_cast<read_request_t*>(req);
    if(r->idx_no!=0){
        curr_element++;
        if(curr_element<curr_numberOfRecs){
            bool found;
            uint pksz = r->table->key_info[0].key_length;
            uchar* tmp_pk_buffer = (uchar*) my_malloc((size_t) pksz, MYF(MY_WME));

            memcpy(tmp_pk_buffer, cursor->elem()+curr_element*pksz+1, pksz);
            w_keystr_t kstr;
            kstr.construct_regularkey(tmp_pk_buffer, pksz);
            smsize_t size=1;
            uchar* buf= (uchar*) my_malloc(size, MYF(MY_WME));
            w_rc_t rc = foster_handle->find_assoc(r->pkstid,kstr, buf, size, found);
            if(rc.is_error() && rc.err_num()==eRECWONTFIT){
                buf= (uchar*) my_realloc(buf, size, MYF(MY_WME));
                foster_handle->find_assoc(r->pkstid, kstr, buf, size,found);
            }
            unpack_row(buf, size, r->mysql_format_buf, r->table);

            return RCOK;
        }else{
            W_COERCE(cursor->next());
            if(!cursor->eof()) {
                curr_numberOfRecs =cursor->elem()[0];
                curr_element=0;

                bool found;
                uint pksz = r->table->key_info[0].key_length;
                uchar *tmp_pk_buffer = (uchar *) my_malloc((size_t) pksz, MYF(MY_WME));

                memcpy(tmp_pk_buffer, cursor->elem()+curr_element*pksz+1, pksz);
                w_keystr_t kstr;
                kstr.construct_regularkey(tmp_pk_buffer, pksz);
                smsize_t size = 1;
                uchar *buf = (uchar *) my_malloc(size, MYF(MY_WME));
                w_rc_t rc = foster_handle->find_assoc(r->pkstid, kstr, buf, size, found);
                if (rc.is_error() && rc.err_num() == eRECWONTFIT) {
                    buf = (uchar *) my_realloc(buf, size, MYF(MY_WME));
                    foster_handle->find_assoc(r->pkstid, kstr, buf, size, found);
                }
                unpack_row(buf, size, r->mysql_format_buf, r->table);
                return RCOK;
            }else{
                return RC(se_TUPLE_NOT_FOUND);
            }
        }
    }else {
        W_COERCE(cursor->next());
        if (!cursor->eof()) {
            unpack_row((uchar *) cursor->elem(), cursor->elen(), r->mysql_format_buf, r->table);
            return RCOK;
        } else {
            return RC(se_TUPLE_NOT_FOUND);
        }
    }
}



w_rc_t fstr_wrk_thr_t::position_read(){
    read_request_t* r = static_cast<read_request_t*>(req);
    bool found;
    w_rc_t rc;
    w_keystr_t kstr;
    kstr.construct_regularkey(r->key_buf, r->ksz);
    uchar* record_buf;
    smsize_t size=1;
    record_buf= (uchar*) my_malloc(size, MYF(MY_WME));
    rc = foster_handle->find_assoc(r->stid,kstr,record_buf,size, found);
    if(rc.is_error() && rc.err_num()==eRECWONTFIT){
        record_buf = (uchar *) my_realloc(record_buf, size, MYF(MY_WME));
        rc = foster_handle->find_assoc(r->stid,kstr,record_buf,size, found);
    }
    unpack_row(record_buf, size, r->mysql_format_buf, r->table);
    return rc;
}



int fstr_wrk_thr_t::add_to_secondary_idx(StoreID sec_id, w_keystr_t sec_kstr, w_keystr_t primary){

    w_rc_t rc;
    bool found;
    smsize_t size=primary.get_length_as_nonkeystr();
    uchar* record_buf= (uchar*) my_malloc(size, MYF(MY_WME | MY_ZEROFILL));
    rc = foster_handle->find_assoc(sec_id,sec_kstr,record_buf,size, found);
    if(rc.is_error() && rc.err_num()==eRECWONTFIT){
        record_buf = (uchar *) my_realloc(record_buf, size, MYF(MY_WME | MY_ZEROFILL));
        rc = foster_handle->find_assoc(sec_id,sec_kstr,record_buf,size, found);
    }
    if(found){
        uint pksz = primary.get_length_as_nonkeystr();
        uint numberOfRecs= record_buf[0]+1;

        uchar* new_buf = (uchar*) my_malloc(numberOfRecs*pksz+1, MYF(MY_WME | MY_ZEROFILL));
        uchar* curr=new_buf;
        *curr++=numberOfRecs;
        uchar* tmp_pk_buffer = (uchar*) my_malloc(pksz, MYF(MY_WME));

        w_keystr_t compare_key;
        record_buf++;

        bool done;

        for(int i=0; i< numberOfRecs-1; i++){
            memcpy(tmp_pk_buffer, record_buf+(i*pksz), pksz);
            compare_key.construct_regularkey(tmp_pk_buffer, pksz);
            if(compare_key.compare(primary)>0 && !done){
                memcpy(curr,primary.serialize_as_nonkeystr().c_str(), pksz);
                curr+=pksz;
                memcpy(curr, tmp_pk_buffer, pksz);
                curr+=pksz;
                done=true;
            }else{
                memcpy(curr, tmp_pk_buffer, pksz);
                curr+=pksz;
            }
        }
        if(!done){
            memcpy(curr,primary.serialize_as_nonkeystr().c_str(),pksz);
        }
        my_free(tmp_pk_buffer);
        foster_handle->put_assoc(sec_id,sec_kstr,vec_t(new_buf, numberOfRecs*pksz+1));
        my_free(new_buf);
        my_free(--record_buf);
        return 0;
    }else{
        record_buf = (uchar*) my_realloc(record_buf, primary.get_length_as_nonkeystr() + 1, MYF(MY_WME));
        //NumberOfRecs
        *record_buf++=1;
        memcpy(record_buf,primary.serialize_as_nonkeystr().c_str(), primary.get_length_as_nonkeystr());
        record_buf--;
        foster_handle->create_assoc(sec_id, sec_kstr, vec_t(record_buf, primary.get_length_as_nonkeystr()+ 1));
        my_free(record_buf);
        return 0;
    }
}

int fstr_wrk_thr_t::delete_from_secondary_idx(StoreID sec_id, w_keystr_t sec_kstr, w_keystr_t primary){
    w_rc_t rc;
    bool found;
    smsize_t size=primary.get_length_as_nonkeystr()+1;
    uchar* record_buf= (uchar*) my_malloc(size, MYF(MY_WME | MY_ZEROFILL));
    rc = foster_handle->find_assoc(sec_id,sec_kstr,record_buf,size, found);
    if(rc.is_error() && rc.err_num()==eRECWONTFIT){
        record_buf = (uchar *) my_realloc(record_buf, size, MYF(MY_WME | MY_ZEROFILL));
        rc = foster_handle->find_assoc(sec_id,sec_kstr,record_buf,size, found);
    }
    if(found){
        uint pksz = primary.get_length_as_nonkeystr();
        uint numberOfRecs= record_buf[0]-1;

        uchar* new_buf = (uchar*) my_malloc(numberOfRecs*pksz+1, MYF(MY_WME | MY_ZEROFILL));
        uchar* curr=new_buf;
        *curr++=numberOfRecs;
        uchar* tmp_pk_buffer = (uchar*) my_malloc(pksz, MYF(MY_WME));

        w_keystr_t compare_key;
        record_buf++;
        for(int i=0; i< numberOfRecs+1; i++){
            memcpy(tmp_pk_buffer, record_buf+(i*pksz), pksz);
            compare_key.construct_regularkey(tmp_pk_buffer, pksz);
            if(compare_key.compare(primary)!=0){
                memcpy(curr, tmp_pk_buffer, pksz);
                curr+=pksz;
            }
        }

        my_free(tmp_pk_buffer);
        foster_handle->put_assoc(sec_id,sec_kstr,vec_t(new_buf, numberOfRecs*pksz+1));
        my_free(new_buf);
        my_free(--record_buf);
        return 0;
    }else{
        return 1;
    }

}


w_rc_t fstr_wrk_thr_t::foster_commit(){
    W_COERCE(foster_handle->commit_xct());
    return RCOK;
}


w_rc_t fstr_wrk_thr_t::foster_rollback(){
    W_COERCE(foster_handle->abort_xct());
    return RCOK;
}