//
// Created by stefan on 16.03.16.
//

#include "fstr_wrk_thr_t.h"

#include "btcursor.h"


ss_m* fstr_wrk_thr_t::foster_handle;

fstr_wrk_thr_t::fstr_wrk_thr_t():
         smthread_t(t_regular, "trx_worker"){
//    init_foster_psi_keys();
    pthread_mutex_init(&thread_mutex, NULL);
    pthread_cond_init(&COND_worker, NULL);
    notified = false;
    aborted=false;
    this->fork();
}



void fstr_wrk_thr_t::foster_exit(){
    pthread_mutex_lock(&thread_mutex);
    notify(true);
    _exit=true;
    pthread_cond_signal(&COND_worker);
    pthread_mutex_unlock(&thread_mutex);
    join();
    pthread_mutex_destroy(&thread_mutex);
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

    options->set_int_option("sm_chkpt_interval", 100000);

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
    int rval=0;
    while(true){
        pthread_mutex_lock(&thread_mutex);
        while(!notified){
            pthread_cond_wait(&COND_worker, &thread_mutex);
        }
        pthread_mutex_unlock(&thread_mutex);
        notified=false;
        if(_exit) return;
        rval = work_ACTIVE();
        if(rval) return;
    }
}


int fstr_wrk_thr_t::work_ACTIVE(){
    w_rc_t err;
    switch(req->type){
        case FOSTER_STARTUP:
            err=startup();break;
        case FOSTER_SHUTDOWN:
            err=shutdown();break;
            case FOSTER_DISCOVERY:
            err=discover_table(static_cast<discovery_request_t*>(req)); break;
        case FOSTER_CREATE:
            err=create_physical_table(static_cast<ddl_request_t*>(req));break;
        case FOSTER_DELETE:
            err=delete_table(static_cast<ddl_request_t*>(req)); break;
        case FOSTER_WRITE_ROW:
            err=add_tuple(static_cast<write_request_t*>(req)); break;
        case FOSTER_UPDATE_ROW:
            update_tuple(static_cast<write_request_t*>(req)); break;
        case FOSTER_DELETE_ROW:
            err=delete_tuple(static_cast<write_request_t*>(req)); break;
        case FOSTER_IDX_READ:
            err=index_probe(static_cast<read_request_t*>(req)); break;
        case FOSTER_IDX_NEXT:
            err=next(static_cast<read_request_t*>(req)); break;
        case FOSTER_POS_READ:
            err=position_read(static_cast<read_request_t*>(req)); break;
        case FOSTER_COMMIT:
            err=foster_commit(); break;
        case FOSTER_ROLLBACK:
            err=foster_rollback();break;
        case FOSTER_BEGIN:
            err=foster_begin(); break;
        default: break;
    }
    req->err=translate_err_code(err.err_num());
    req->notified=true;
    pthread_cond_signal(&req->COND_work);
    return 0;
}

w_rc_t fstr_wrk_thr_t::shutdown() {
    delete(foster_handle);
    return RCOK;
}

w_rc_t fstr_wrk_thr_t::startup() {
    sm_options* options = new sm_options();
    foster_config(options);
    foster_handle = new ss_m(*options);
    vol_t *vol = ss_m::vol;
    w_assert0(vol);
    if (!vol->is_alloc_store(1))
    {
        StoreID cat_stid =1;
        W_COERCE(foster_begin());
        W_COERCE(foster_handle->create_index(cat_stid));
        w_assert0(cat_stid == 1);
        W_COERCE(foster_commit());
    }
    return RCOK;
}


w_keystr_t foster_key_copy(uchar *to_key, uchar *from_record, FosterIndexInfo::Reader *key_info)
{
    w_keystr_t ret;
    uint key_length = key_info->getKeylength();

    int blob_length=2;
    uchar* keypos= to_key;
    ::capnp::List<FosterFieldInfo>::Reader parts = key_info->getPartinfo();
    for(uint i=0; i< parts.size(); i++){
        FosterFieldInfo::Reader key_part = parts[i];
        if(key_part.getNullBit()!=0){
            //Dont copy null values
            if((from_record[key_part.getNullOffset()] & key_part.getNullBit())) {
                *keypos++=1;
                bzero((char*) keypos, key_length-1);
                continue;
            }else{
                *keypos++=0;
            }
        }

        if(key_part.getBlob()|| key_part.getVarlength()){
            //the actual length of the varchar can also be stored in 2 bytes
            uint actualLength = (uint) from_record[key_part.getOffset()];
            uchar *pos= &from_record[key_part.getOffset()]+1;
            *((uint16_t*) (keypos)) = (uint16_t) from_record[key_part.getOffset()];
            memcpy(keypos+2,pos, actualLength);
            if(actualLength < key_part.getLength()) {
                bzero(keypos + 2 + actualLength, (key_part.getLength() - actualLength));
            }
            keypos+=key_part.getLength()+2;
        }else{
            uchar* record_key_pos= &from_record[key_part.getOffset()];
            memcpy(keypos,record_key_pos, key_part.getLength());
            keypos+=key_part.getLength();
        }
    }
    ret.construct_regularkey(to_key,key_length);
    return ret;
}

w_rc_t fstr_wrk_thr_t::create_physical_table(ddl_request_t* r){
    StoreID cat_stid =1;
    StoreID primary_stid;
    W_COERCE(foster_begin());
    //Create the primary index
    W_COERCE(foster_handle->create_index(primary_stid));
    //Create Primary Key String from name of the database and table_name
    w_keystr_t cat_entry_key;
    construct_cat_key(
            const_cast<char*>(r->capnpTable.getDbname().asString().cStr()),
            (uint) r->capnpTable.getDbname().asString().size(),
            const_cast<char*>(r->capnpTable.getDbname().asString().cStr()),
            (uint) r->capnpTable.getDbname().asString().size(), cat_entry_key
    );
    ::capnp::List<FosterIndexInfo>::Builder indexes = r->capnpTable.getIndexes();
    FosterIndexInfo::Builder primary_idx = indexes[0];
    primary_idx.setStid(primary_stid);
    primary_idx.setPrimary(primary_stid);
    for(uint i=1; i<indexes.size(); i++){
        StoreID sec_stid;
        W_COERCE(foster_handle->create_index(sec_stid));
        FosterIndexInfo::Builder curr_index = indexes[i];
        curr_index.setStid(sec_stid);
        curr_index.setPrimary(primary_stid);
    }

    if(!r->foreign_keys.empty()) {
        ::capnp::List<FosterForeignTable>::Builder foreignKeys = r->capnpTable.initForeignkeys(
                r->foreign_keys.size());
        std::list<FOSTER_FK_INFO>::iterator fk_iterator;
        for(uint i=0; i<r->foreign_keys.size(); i++){
            FosterForeignTable::Builder proto_curr_fk = foreignKeys[i];
            FOSTER_FK_INFO* curr_fk_req= &r->foreign_keys.at(i);
            proto_curr_fk.setId(curr_fk_req->foreign_id);
            w_keystr_t ref_keystr;
            construct_cat_key(
                    const_cast<char*>(curr_fk_req->referenced_db.c_str()),
                    curr_fk_req->referenced_db.length(),
                    const_cast<char*>(curr_fk_req->referenced_table.c_str()),
            curr_fk_req->referenced_table.length(), ref_keystr);
            proto_curr_fk.setForeignTableId((char*) ref_keystr.serialize_as_nonkeystr().c_str());

            bool found;
            smsize_t ref_buf_size=1;
            capnp::word *ref_buf = (capnp::word*) malloc(ref_buf_size);
            w_rc_t err;
            err = ss_m::find_assoc(cat_stid, ref_keystr, ref_buf, ref_buf_size, found);
            if(!found) continue; //TODO errmsg -> referenced table not found
            if(err.is_error() && err.err_num() == eRECWONTFIT){
                ref_buf= (capnp::word*) realloc(ref_buf, ref_buf_size);
                ss_m::find_assoc(cat_stid, ref_keystr, ref_buf, ref_buf_size, found);
            }
            kj::ArrayPtr<const capnp::word> ptr= kj::arrayPtr(reinterpret_cast<const capnp::word*>(ref_buf),
                                                              ref_buf_size/ sizeof(capnp::word));
            capnp::FlatArrayMessageReader refreader(ptr);
            FosterTableInfo::Reader refTableInfo = refreader.getRoot<FosterTableInfo>();
            if(found){
                uint fk_index_pos;
                string fk_index_name;
                uint ref_index_pos;
                string ref_index_name;
                uint ref_stid;

                proto_curr_fk.setForeignTableId((char*) ref_keystr.serialize_as_nonkeystr().c_str());
                capnp::List<FosterIndexInfo>::Reader refindexes = refTableInfo.getIndexes();


                //Find referenced index
                for(uint curr_ref_idx=0; curr_ref_idx<refindexes.size();curr_ref_idx++){
                    FosterIndexInfo::Reader curr_index = refindexes[curr_ref_idx];
                    if(curr_fk_req->referenced_fields.size()!=
                            curr_index.getPartinfo().size()) continue;
                    bool fk_index_found= true;
                    capnp::List<FosterFieldInfo>::Reader refparts = curr_index.getPartinfo();
                    for(uint j=0; j<refparts.size(); j++){
                        FosterFieldInfo::Reader curr_part = refparts[j];
                        if(strcmp(curr_part.getFieldname().cStr(),curr_fk_req->referenced_fields.at(j).c_str())!=0){
                            fk_index_found=false;
                            break;
                        }
                    }
                    if(fk_index_found){
                        ref_index_pos=curr_ref_idx;
                        ref_index_name=curr_index.getIndexname();
                        ref_stid = curr_index.getStid();
                        break;
                    }
                }
                //Find foreign index
                for(uint curr_foreign_idx=0; curr_foreign_idx<indexes.size();curr_foreign_idx++){
                    FosterIndexInfo::Reader curr_index = indexes[curr_foreign_idx];
                    if(curr_fk_req->foreign_fields.size()!=
                       curr_index.getPartinfo().size()) continue;
                    bool fk_index_found= true;
                    capnp::List<FosterFieldInfo>::Reader parts = curr_index.getPartinfo();
                    for(uint j=0; j<parts.size(); j++){
                        FosterFieldInfo::Reader curr_part = parts[j];
                        if(strcmp(curr_part.getFieldname().cStr(), curr_fk_req->foreign_fields.at(j).c_str())!=0){
                            fk_index_found=false;
                            break;
                        }
                    }
                    if(fk_index_found){
                        fk_index_pos=curr_foreign_idx;
                        fk_index_name=curr_index.getIndexname();
                        break;
                    }
                }

                //Build new foreign data
                proto_curr_fk.setForeignIdx(fk_index_name);
                proto_curr_fk.setForeignIdxPos(fk_index_pos);
                proto_curr_fk.setReferencingIdxPos(ref_index_pos);
                proto_curr_fk.setReferencingIdx(ref_index_name);
                proto_curr_fk.setReferencingStid(ref_stid);
                //Build new referencing data
                uint insert_pos;
                capnp::MallocMessageBuilder mo;
                ::capnp::initMessageBuilderFromFlatArrayCopy(ptr, mo);
                FosterTableInfo::Builder refTable_new = mo.getRoot<FosterTableInfo>();
                if(refTableInfo.hasReferencingKeys()) {
                    ::capnp::Orphan< ::capnp::List<::FosterReferencingTable>> old_refRef= refTable_new.disownReferencingKeys();
                    ::capnp::List<FosterReferencingTable>::Reader old_refRefList= old_refRef.get();
                    capnp::List<FosterReferencingTable>::Builder refReferencing = refTable_new.initReferencingKeys(
                            old_refRefList.size() + 1);
                    for (uint k = 0; k < old_refRefList.size(); k++) {
                        FosterReferencingTable::Reader copy_ref_reader = old_refRefList[k];
                        FosterReferencingTable::Builder copy_ref_builder = refReferencing[k];
                        copy_ref_builder.setReferencingTable(copy_ref_reader.getReferencingTable());
                        copy_ref_builder.setReferencingIdx(copy_ref_reader.getReferencingIdx());
                        copy_ref_builder.setReferencingIdxPos(copy_ref_reader.getReferencingIdxPos());
                        copy_ref_builder.setForeignIdxPos(copy_ref_reader.getForeignIdxPos());
                        copy_ref_builder.setForeignIdx(copy_ref_reader.getForeignIdx());
                        copy_ref_builder.setId(copy_ref_reader.getId());
                        copy_ref_builder.setAction(copy_ref_reader.getAction());
                        copy_ref_builder.setType(copy_ref_reader.getType());
                    }
                    insert_pos= old_refRefList.size()+1;
                }else{
                    insert_pos=1;
                    refTable_new.initReferencingKeys(1);
                }
                capnp::List<FosterReferencingTable>::Builder refReferencing = refTable_new.getReferencingKeys();
                FosterReferencingTable::Builder proto_curr_refRef = refReferencing[insert_pos-1];
                proto_curr_refRef.setReferencingTable((char*) cat_entry_key.serialize_as_nonkeystr().c_str());
                proto_curr_refRef.setReferencingIdx(ref_index_name);
                proto_curr_refRef.setReferencingIdxPos(ref_index_pos);
                proto_curr_refRef.setForeignIdxPos(fk_index_pos);
                proto_curr_refRef.setForeignIdx(fk_index_name);
                proto_curr_refRef.setId(curr_fk_req->foreign_id);
                switch(curr_fk_req->delete_method) {
                    case 1: proto_curr_refRef.setType(FosterReferencingTable::Type::DELETE);
                        proto_curr_refRef.setAction(FosterReferencingTable::Action::CASCADE);break;
                    case 2: proto_curr_refRef.setType(FosterReferencingTable::Type::DELETE);
                        proto_curr_refRef.setAction(FosterReferencingTable::Action::RESTRICT);break;
                    case 3: proto_curr_refRef.setType(FosterReferencingTable::Type::DELETE);
                        proto_curr_refRef.setAction(FosterReferencingTable::Action::SETNULL);break;
                    case 4: proto_curr_refRef.setType(FosterReferencingTable::Type::DELETE);
                        proto_curr_refRef.setAction(FosterReferencingTable::Action::NOACTION);break;
                    default: break;
                }
                switch(curr_fk_req->update_method) {
                    case 1: proto_curr_refRef.setType(FosterReferencingTable::Type::UPDATE);
                        proto_curr_refRef.setAction(FosterReferencingTable::Action::CASCADE);break;
                    case 2: proto_curr_refRef.setType(FosterReferencingTable::Type::UPDATE);
                        proto_curr_refRef.setAction(FosterReferencingTable::Action::RESTRICT);break;
                    case 3: proto_curr_refRef.setType(FosterReferencingTable::Type::UPDATE);
                        proto_curr_refRef.setAction(FosterReferencingTable::Action::SETNULL);break;
                    case 4: proto_curr_refRef.setType(FosterReferencingTable::Type::UPDATE);
                        proto_curr_refRef.setAction(FosterReferencingTable::Action::NOACTION);break;
                    default: break;
                }
                kj::Array<capnp::word> ref_words = messageToFlatArray(mo);
                kj::ArrayPtr<kj::byte> ref_bytes = ref_words.asBytes();
                kj::ArrayPtr<const capnp::word> refTableArrayPtr= kj::arrayPtr(reinterpret_cast<const capnp::word*>(ref_bytes.begin()),
                                                                   ref_bytes.size() / sizeof(capnp::word));
                err =foster_handle->put_assoc(cat_stid, ref_keystr,
                                              vec_t(ref_bytes.begin(),ref_bytes.size()));
                curr_fk_req->reftables = kj::mv(refTableArrayPtr);
            }
        }
    }
    w_rc_t err;
    kj::Array<capnp::word> words = messageToFlatArray(r->message);
    kj::ArrayPtr<kj::byte> bytes = words.asBytes();
//    print_capnp_table(r->capnpTable);
    err =foster_handle->create_assoc(cat_stid, cat_entry_key,
                                         vec_t(bytes.begin(),bytes.size()));
    W_COERCE(foster_commit());
    return err;
}


w_rc_t fstr_wrk_thr_t::discover_table(discovery_request_t* r)
{
    w_keystr_t cat_entry_key ;
    construct_cat_key(const_cast<char*>(r->db_name.c_str()),
            r->db_name.length(), const_cast<char*>(r->table_name.c_str()),
                      r->table_name.length(), cat_entry_key);
    StoreID cat_stid =1;
    bool found;
    smsize_t size=1;
    capnp::word *buf = (capnp::word*) malloc(size);
    w_rc_t err;
    err = ss_m::find_assoc(cat_stid, cat_entry_key, buf, size, found);
    if(!found) return RC(eINTERNAL);
    if(err.is_error() && err.err_num() == eRECWONTFIT){
        buf= (capnp::word*) realloc(buf, size);
        ss_m::find_assoc(cat_stid, cat_entry_key, buf, size, found);
    }
    kj::ArrayPtr<const capnp::word> table_array_ptr= kj::arrayPtr(
                reinterpret_cast<const capnp::word*>(buf),
                                                           size/ sizeof(capnp::word));
    r->table_info_array = kj::mv(table_array_ptr);
    return RCOK;
}

w_rc_t fstr_wrk_thr_t::delete_table(ddl_request_t* r) {
    StoreID cat_stid =1;
    w_rc_t err;
    w_keystr_t cat_entry_key;
    construct_cat_key(const_cast<char*>(r->db_name.c_str()),
                      r->db_name.length(),
                      const_cast<char*>(r->table_name.c_str()),
                      r->table_name.length(), cat_entry_key);
    W_COERCE(foster_begin());
    err=foster_handle->destroy_assoc(cat_stid,cat_entry_key);
    W_COERCE(foster_commit());
    return err;
}

#include <btree.h>

w_rc_t fstr_wrk_thr_t::add_tuple(write_request_t* r){
    w_rc_t rc = RCOK;
    if(r->table_info_array.size()==0) return RC(eINTERNAL);
    capnp::FlatArrayMessageReader fareader(r->table_info_array);
    FosterTableInfo::Reader table_reader = fareader.getRoot<FosterTableInfo>();
    capnp::List<FosterIndexInfo>::Reader indexes = table_reader.getIndexes();
    capnp::List<FosterForeignTable>::Reader foreign_keys= table_reader.getForeignkeys();
    if(foreign_keys.size()>0){
        StoreID cat_stid=1;
        for(uint i=0; i< foreign_keys.size(); i++ ) {
            FosterForeignTable::Reader curr_foreign = foreign_keys[i];
            StoreID ref_stid= curr_foreign.getReferencingStid();
            FosterIndexInfo::Reader foreignIdx = indexes[curr_foreign.getForeignIdxPos()];
            w_keystr_t foreign_kstr=foster_key_copy(r->key_buf, r->mysql_format_buf, &foreignIdx);
            w_keystr_t infimum;
            infimum.construct_posinfkey();
            bt_cursor_t* ref_cursor = new bt_cursor_t(ref_stid,
                                                      foreign_kstr,
                                                          true,
                                                          infimum,
                                                          false,
                                                          true);
            W_COERCE(ref_cursor->next());
            if(ref_cursor->eof()) return RC(fcNOTIMPLEMENTED);
            int d = memcmp(foreign_kstr.buffer_as_keystr(),
                           ref_cursor->key().buffer_as_keystr(),
                           foreign_kstr.get_length_as_keystr());
            if(d!=0) return RC(fcNOTIMPLEMENTED); //TODO better error codes
        }
    }

    for(int i=0; i<indexes.size();i++){
        PageID root_pid;
        FosterIndexInfo::Reader index = indexes[i];
        foster_handle->open_store(index.getStid(),root_pid,true);
    }
    FosterIndexInfo::Reader primaryIdx = table_reader.getIndexes()[0];
    StoreID pkstid = table_reader.getIndexes()[0].getStid();
    w_keystr_t kstr=foster_key_copy(r->key_buf, r->mysql_format_buf, &primaryIdx);
    //create assoc in primary
    rc=foster_handle->bt->insert(pkstid, kstr, vec_t(r->packed_record_buf, r->packed_len));

    if(rc.is_error()) { return rc;}
    if(table_reader.getIndexes().size()>1){
        ::capnp::List<FosterIndexInfo>::Reader indexes = table_reader.getIndexes();
        for (uint idx = 1; idx < indexes.size(); idx++) {
            FosterIndexInfo::Reader curr_index = indexes[idx];
            StoreID sec_idx_stid = curr_index.getStid();
            w_keystr_t sec_kstr=foster_key_copy(r->key_buf, r->mysql_format_buf, &curr_index);
            add_to_secondary_idx_uniquified(sec_idx_stid, sec_kstr, kstr);
        }
    }
    return (rc);
}

w_rc_t fstr_wrk_thr_t::update_tuple(write_request_t* r){
    w_rc_t rc;
    if(r->table_info_array.size()==0) return RC(eINTERNAL);
    capnp::FlatArrayMessageReader fareader(r->table_info_array);
    FosterTableInfo::Reader table_reader = fareader.getRoot<FosterTableInfo>();
    bool changed_pk=false;
    capnp::List<FosterIndexInfo>::Reader indexes = table_reader.getIndexes();
    capnp::List<FosterForeignTable>::Reader foreign_keys= table_reader.getForeignkeys();
    if(foreign_keys.size()>0){
        StoreID cat_stid=1;
        for(uint i=0; i< foreign_keys.size(); i++ ) {
            FosterForeignTable::Reader curr_foreign = foreign_keys[i];
            StoreID ref_stid= curr_foreign.getReferencingStid();
            FosterIndexInfo::Reader foreignIdx = indexes[curr_foreign.getForeignIdxPos()];
            w_keystr_t foreign_kstr=foster_key_copy(r->key_buf, r->mysql_format_buf, &foreignIdx);
            w_keystr_t infimum;
            infimum.construct_posinfkey();
            bt_cursor_t* ref_cursor = new bt_cursor_t(ref_stid,
                                                      foreign_kstr,
                                                      true,
                                                      infimum,
                                                      false,
                                                      true);
            W_COERCE(ref_cursor->next());
            if(ref_cursor->eof()) return RC(fcNOTIMPLEMENTED);
            int d = memcmp(foreign_kstr.buffer_as_keystr(),
                           ref_cursor->key().buffer_as_keystr(),
                           foreign_kstr.get_length_as_keystr());
            if(d!=0) return RC(fcNOTIMPLEMENTED); //TODO better error codes
        }
    }

    for(int i=0; i<indexes.size();i++){
        PageID root_pid;
        FosterIndexInfo::Reader index = indexes[i];
        foster_handle->open_store(index.getStid(),root_pid,true);
    }
    FosterIndexInfo::Reader primaryIdx = table_reader.getIndexes()[0];

    StoreID pkstid = table_reader.getIndexes()[0].getStid();
    w_keystr_t new_pk_kstr=foster_key_copy(r->key_buf, r->mysql_format_buf, &primaryIdx);
    w_keystr_t old_pk_kstr=foster_key_copy(r->key_buf, r->old_mysql_format_buf, &primaryIdx);

    if(old_pk_kstr.compare(new_pk_kstr)!=0){
        rc=foster_handle->bt->insert(pkstid, new_pk_kstr, vec_t(r->packed_record_buf, r->packed_len));
        rc=foster_handle->bt->remove(pkstid, old_pk_kstr);
        changed_pk=true;
    }else{
        rc=foster_handle->bt->put(pkstid,new_pk_kstr, vec_t(r->packed_record_buf, r->packed_len));
    }
    if(indexes.size()>1){
        bool changed_sec=false;
        for (uint idx = 1; idx < indexes.size(); idx++) {
            FosterIndexInfo::Reader curr_index= indexes[idx];
            StoreID sec_idx_stid = curr_index.getStid();
            w_keystr_t new_sec_kstr= foster_key_copy(r->key_buf, r->mysql_format_buf, &curr_index);
            w_keystr_t old_sec_kstr= foster_key_copy(r->key_buf, r->old_mysql_format_buf, &curr_index);
           if(old_sec_kstr.compare(new_sec_kstr)!=0){
                changed_sec=true;
            }
            if(changed_pk || changed_sec){
//                delete_from_secondary_idx(sec_idx_stid,old_sec_kstr, old_pk_kstr);
//                add_to_secondary_idx(sec_idx_stid,new_sec_kstr,new_pk_kstr);

                delete_from_secondary_idx_uniquified(sec_idx_stid, old_sec_kstr, old_pk_kstr);
                add_to_secondary_idx_uniquified(sec_idx_stid,new_sec_kstr, new_pk_kstr);
            }
        }
    }
    return rc;
}

w_rc_t fstr_wrk_thr_t::delete_tuple(write_request_t* r){
    if(r->table_info_array.size()==0) return RC(eINTERNAL);
    capnp::FlatArrayMessageReader fareader(r->table_info_array);
    FosterTableInfo::Reader table_reader = fareader.getRoot<FosterTableInfo>();
    w_rc_t rc;
    FosterIndexInfo::Reader primaryIdx = table_reader.getIndexes()[0];
    capnp::List<FosterIndexInfo>::Reader indexes = table_reader.getIndexes();
    for(int i=0; i<indexes.size();i++){
        PageID root_pid;
        FosterIndexInfo::Reader index = indexes[i];
        rc=foster_handle->open_store(index.getStid(),root_pid,true);
    }
    w_keystr_t primary_kstr=foster_key_copy(r->key_buf, r->mysql_format_buf, &primaryIdx);
    rc=foster_handle->bt->remove(indexes[0].getStid(), primary_kstr);
    if(indexes.size()>1){
        for (uint idx = 1; idx < indexes.size(); idx++) {
            FosterIndexInfo::Reader curr_index= indexes[idx];
            w_keystr_t sec_kstr= foster_key_copy(r->key_buf, r->mysql_format_buf, &curr_index);
            StoreID sec_idx_stid = curr_index.getStid();
//            delete_from_secondary_idx(sec_idx_stid,sec_kstr, primary_kstr);

            delete_from_secondary_idx_uniquified(sec_idx_stid, sec_kstr, primary_kstr);
        }
    }
    //TODO open all stores of referenced tables
    capnp::List<FosterReferencingTable>::Reader ref_keys= table_reader.getReferencingKeys();
    if(ref_keys.size()>0){
        StoreID cat_stid=1;
        for(uint i=0; i< ref_keys.size(); i++ ){
            FosterReferencingTable::Reader curr_ref = ref_keys[i];
            if(curr_ref.getType()==FosterReferencingTable::Type::DELETE) {
                FosterIndexInfo::Reader referenced_idx = table_reader.getIndexes()[curr_ref.getReferencingIdxPos()];
                smsize_t fktable_cat_sz=1;
                //Get information about the referencing (i.e. foreign) table from the catalog
                w_keystr_t fktable_cat_keystr;
                fktable_cat_keystr.construct_regularkey(curr_ref.getReferencingTable().cStr(), curr_ref.getReferencingTable().size());
                bool found;
                capnp::word *ref_buf = (capnp::word *) malloc(fktable_cat_sz);
                w_rc_t err;
                err = ss_m::find_assoc(cat_stid, fktable_cat_keystr, ref_buf, fktable_cat_sz, found);
                if(!found) continue; //TODO errmsg missing foreign table
                if (err.is_error() && err.err_num() == eRECWONTFIT) {
                    ref_buf = (capnp::word *) realloc(ref_buf, fktable_cat_sz);
                    ss_m::find_assoc(cat_stid, fktable_cat_keystr, ref_buf, fktable_cat_sz, found);
                }
                kj::ArrayPtr<const capnp::word> foreignTable_Array =
                        kj::arrayPtr(reinterpret_cast<const capnp::word *>(ref_buf),
                                                                                  fktable_cat_sz / sizeof(capnp::word));
                capnp::FlatArrayMessageReader refreader(foreignTable_Array);
                FosterTableInfo::Reader foreignTableInfo = refreader.getRoot<FosterTableInfo>();

                //Get Primary Keys from the secondary idx that is the foreign idx
                FosterIndexInfo::Reader foreign_idx =
                        foreignTableInfo.getIndexes()[curr_ref.getForeignIdxPos()];

                //Get the foreign key that needs to be deleted from the referenced idx
                w_keystr_t foreign_key_kstr=
                        foster_key_copy(r->key_buf, r->mysql_format_buf, &referenced_idx);
                w_keystr_t infimum;
                infimum.construct_posinfkey();
                bt_cursor_t* foreign_cursor = new bt_cursor_t(foreign_idx.getStid(),
                                                             foreign_key_kstr,
                                                             true,
                                                             infimum,
                                                             false,
                                                             true);

//                smsize_t foreign_sec_tuple_sz = 1;
//                uchar *foreign_sec_tuple = (uchar *) malloc(foreign_sec_tuple_sz);
//                err = ss_m::find_assoc(foreign_idx.getStid(), foreign_key_kstr,
//                                       foreign_sec_tuple, foreign_sec_tuple_sz, found);
//                if(!found) continue; //TODO errmsg corrupted foreign idx
//                if (err.is_error() && err.err_num() == eRECWONTFIT) {
//                    ref_buf = (capnp::word *) realloc(foreign_sec_tuple, foreign_sec_tuple_sz);
//                    ss_m::find_assoc(foreign_idx.getStid(),
//                                     foreign_key_kstr, foreign_sec_tuple, foreign_sec_tuple_sz, found);
//                }
                W_COERCE(foreign_cursor->next());
                if(foreign_cursor->eof()) continue; //TODO errmsg corrupted foreign idx
                int d = memcmp(foreign_key_kstr.buffer_as_keystr(),
                               foreign_cursor->key().buffer_as_keystr(),
                               foreign_key_kstr.get_length_as_keystr());

                FosterIndexInfo::Reader foreign_primary_idx = foreignTableInfo.getIndexes()[0];
                smsize_t foreign_tuple_sz=1;
                uchar* foreign_tuple_buf= (uchar*) malloc(foreign_tuple_sz);

                while(d==0){
                    w_keystr_t curr_foreign_primary_kstr;
                    curr_foreign_primary_kstr.construct_regularkey(foreign_cursor->elem(),
                                                                   foreign_cursor->elen());
                    err = ss_m::find_assoc(foreign_primary_idx.getStid(),
                                           curr_foreign_primary_kstr, foreign_tuple_buf,
                                           foreign_tuple_sz, found);
                    if (err.is_error() && err.err_num() == eRECWONTFIT) {
                        foreign_tuple_buf = (uchar*) realloc(foreign_tuple_buf, foreign_tuple_sz);
                        err = ss_m::find_assoc(foreign_primary_idx.getStid(), curr_foreign_primary_kstr,
                                               foreign_tuple_buf, foreign_tuple_sz, found);
                    }

                    if(found){
                        switch(curr_ref.getAction()){
                            case capnp::schemas::Action_df51f652b4df6b49::CASCADE: {
                                write_request_t *delete_foreign_req = new write_request_t();
                                delete_foreign_req->mysql_format_buf = foreign_tuple_buf;
                                delete_foreign_req->key_buf = r->key_buf;
                                delete_foreign_req->table_info_array = foreignTable_Array;
                                delete_tuple(delete_foreign_req);
                                break;
                            }
                            case capnp::schemas::Action_df51f652b4df6b49::RESTRICT:
                                return RC(fcNOTIMPLEMENTED); break;
                            case capnp::schemas::Action_df51f652b4df6b49::NOACTION:break;
                            case capnp::schemas::Action_df51f652b4df6b49::SETDEFAULT:break;
                            case capnp::schemas::Action_df51f652b4df6b49::SETNULL:break;
                        }
                    }

                    W_COERCE(foreign_cursor->next());
                    if(foreign_cursor->eof()) break;
                    d = memcmp(foreign_key_kstr.buffer_as_keystr(),
                                   foreign_cursor->key().buffer_as_keystr(),
                                   foreign_key_kstr.get_length_as_keystr());
                }

//                uint numberOfRecs= foreign_sec_tuple[0];
//                for(uchar* pos=++foreign_sec_tuple;
//                    pos< (foreign_sec_tuple +foreign_sec_tuple_sz);
//                    pos+= foreign_primary_idx.getKeylength()){
//                    w_keystr_t curr_foreign_primary_kstr;
//                    curr_foreign_primary_kstr.construct_regularkey(pos, foreign_primary_idx.getKeylength());
//
//
//                }
                free(foreign_tuple_buf);
            }

        }
    }
    return (rc);
}



w_rc_t fstr_wrk_thr_t::index_probe(read_request_t* r){
    if(r->table_info_array.size()==0) return RC(eINTERNAL);
    capnp::FlatArrayMessageReader fareader(r->table_info_array);
    FosterTableInfo::Reader table_reader = fareader.getRoot<FosterTableInfo>();
    //Infimum and supremum for use in cursors
    w_keystr_t infimum, supremum;
    infimum.construct_posinfkey();
    supremum.construct_neginfkey();
    // Build key data
    w_keystr_t kstr;

    bool partial =false;
    //Construct key of tuple in _key_b
    capnp::List<FosterIndexInfo>::Reader indexes = table_reader.getIndexes();
    FosterIndexInfo::Reader primaryIdxInfo = indexes[0];
    if(r->ksz!=0){
            kstr.construct_regularkey(r->key_buf, r->ksz);
        if(r->ksz <indexes[r->idx_no].getKeylength()) {
            partial = true;
            //Null bit set
            if (r->key_buf[0] != 0) kstr.construct_neginfkey();
        }
        PageID root;
        foster_handle->open_store(indexes[0].getStid(), root, true);
    }else{
        r->find_flag==PREFIX_LAST ? kstr.construct_posinfkey() : kstr.construct_neginfkey();
    }

    StoreID idx_stid = indexes[r->idx_no].getStid();
    //Switch on search modes
    switch (r->find_flag) {
        case KEY_OR_NEXT:
            cursor = new bt_cursor_t(idx_stid, kstr, true, infimum, false, true);
            W_COERCE(cursor->next());
            if (cursor->eof()) {
                return RC(se_TUPLE_NOT_FOUND);
            }
            break;
        case KEY_OR_PREV:
            cursor = new bt_cursor_t(idx_stid, supremum, true, kstr, false, false);
            W_COERCE(cursor->next());
            if (cursor->eof()) {
                return RC(se_TUPLE_NOT_FOUND);
            }
            break;
        case KEY_EXACT: {
            _find_exact=true;
            cursor = new bt_cursor_t(idx_stid, kstr, true, infimum, false, true);
            W_COERCE(cursor->next());
            if(!cursor->eof()){
                int d= memcmp(kstr.buffer_as_keystr(),cursor->key().buffer_as_keystr(),
                       kstr.get_length_as_keystr());
                if (d==0) {
                    if(r->idx_no==0 &&
                            kstr.get_length_as_keystr()!= cursor->key().get_length_as_keystr())
                        return RC(se_TUPLE_NOT_FOUND);
                }else{
                    return RC(se_TUPLE_NOT_FOUND);
                }
            }else{
                return RC(se_TUPLE_NOT_FOUND);
            }
        }
            break;
        case AFTER_KEY: {
            cursor = new bt_cursor_t(idx_stid, kstr, false, infimum, false, true);
            W_COERCE(cursor->next());
            if (cursor->eof()) {
                return RC(se_TUPLE_NOT_FOUND);
            }
            int d = memcmp(kstr.buffer_as_keystr(), cursor->key().buffer_as_keystr(),
                           kstr.get_length_as_keystr());
            while (d == 0 && !cursor->eof()) {
                W_COERCE(cursor->next());
                d = memcmp(kstr.buffer_as_keystr(), cursor->key().buffer_as_keystr(),
                           kstr.get_length_as_keystr());
            }

        }
            break;
        case BEFORE_KEY: {
            cursor = new bt_cursor_t(idx_stid, supremum, false, kstr, false, false);
            W_COERCE(cursor->next());
            if (cursor->eof()) {
                return RC(se_TUPLE_NOT_FOUND);
            }
            int d = memcmp(kstr.buffer_as_keystr(), cursor->key().buffer_as_keystr(),
                           kstr.get_length_as_keystr());
            while (d == 0 && !cursor->eof()) {
                W_COERCE(cursor->next());
                d = memcmp(kstr.buffer_as_keystr(), cursor->key().buffer_as_keystr(),
                           kstr.get_length_as_keystr());
            }
        }
            break;
        default:
            return (RCOK);
    }

    if(r->idx_no==0) {
        r->packed_record_buf= (uchar*) cursor->elem();
        r->packed_len= (uint) cursor->elen();
    }else{
        bool found;

        uint pksz = indexes[0].getKeylength();

        curr_numberOfRecs = (uint) cursor->elem()[0];
        curr_element=0;

        w_keystr_t primarykstr;
        primarykstr.construct_regularkey(cursor->elem(), cursor->elen());
        smsize_t size=1;
        uchar* buf= (uchar*) malloc(size);
        w_rc_t rc = foster_handle->find_assoc(indexes[0].getStid(),primarykstr,
                                              buf, size, found);
        if(!found) return RC(eINTERNAL); //TODO err msg corrupted index
        if(rc.is_error() && rc.err_num()==eRECWONTFIT){
            buf= (uchar*) realloc(buf, size);
            foster_handle->find_assoc(indexes[0].getStid(), primarykstr,
                                      buf, size,found);
        }
        r->packed_record_buf= buf;
        r->packed_len=size;
    }
    return (RCOK);

}

w_rc_t fstr_wrk_thr_t::next(read_request_t* r){
    if(r->table_info_array.size()==0) return RC(eINTERNAL);
    capnp::FlatArrayMessageReader fareader(r->table_info_array);
    FosterTableInfo::Reader table_reader = fareader.getRoot<FosterTableInfo>();
    capnp::List<FosterIndexInfo>::Reader indexes = table_reader.getIndexes();
    if(r->idx_no!=0){

        bool found;
        cursor->next();
        if(cursor->eof())
            return  RC(se_TUPLE_NOT_FOUND);

        int d = memcmp(cursor->_lower.buffer_as_keystr(),cursor->key().buffer_as_keystr(),
                       cursor->_lower.get_length_as_keystr());
        if(d!=0 && _find_exact) return RC(se_TUPLE_NOT_FOUND);
        w_keystr_t primarykstr;
        primarykstr.construct_regularkey(cursor->elem(), cursor->elen());
        smsize_t size=1;
        uchar* buf= (uchar*) malloc(size);
        w_rc_t rc = foster_handle->find_assoc(indexes[0].getStid(),primarykstr,
                                              buf, size, found);
        if(!found) return RC(eINTERNAL); //TODO err msg corrupted index
        if(rc.is_error() && rc.err_num()==eRECWONTFIT){
            buf= (uchar*) realloc(buf, size);
            foster_handle->find_assoc(indexes[0].getStid(), primarykstr,
                                      buf, size,found);
        }
        r->packed_record_buf= buf;
        r->packed_len=size;

//        curr_element++;
//        uint pksz=indexes[0].getKeylength();
//        smsize_t size=1;
//        uchar* buf= (uchar*) malloc(size);
//        bool found;
//        w_keystr_t kstr;
//        if(curr_element<curr_numberOfRecs){
//
//            kstr.construct_regularkey(cursor->elem()+curr_element*pksz+1, pksz);
//
//            w_rc_t rc = foster_handle->find_assoc(indexes[0].getStid(),kstr, buf, size, found);
//            if(rc.is_error() && rc.err_num()==eRECWONTFIT){
//                buf= (uchar*) realloc(buf, size);
//                foster_handle->find_assoc(indexes[0].getStid(), kstr, buf, size,found);
//            }
//            r->packed_record_buf= buf;
//            r->packed_len=size;
//        }else{
//            W_COERCE(cursor->next());
//            if(!cursor->eof()) {
//                curr_numberOfRecs =(uint) cursor->elem()[0];
//                curr_element=0;
//                kstr.construct_regularkey( cursor->elem()+curr_element*pksz+1, pksz);
//                w_rc_t rc = foster_handle->find_assoc(indexes[0].getStid(), kstr, buf, size, found);
//                if (rc.is_error() && rc.err_num() == eRECWONTFIT) {
//                    buf = (uchar *) realloc(buf, size);
//                    foster_handle->find_assoc(indexes[0].getStid(), kstr, buf, size, found);
//                }
//                r->packed_record_buf= buf;
//                r->packed_len=size;
//            }else{
//                return RC(se_TUPLE_NOT_FOUND);
//            }
//        }
    }else {
        W_COERCE(cursor->next());
        if (cursor->eof())
            return RC(se_TUPLE_NOT_FOUND);

        r->packed_record_buf= (uchar*) cursor->elem();
        r->packed_len= (uint) cursor->elen();
    }
    return RCOK;
}



w_rc_t fstr_wrk_thr_t::position_read(read_request_t* r){
    if(r->table_info_array.size()==0) return RC(eINTERNAL);
    capnp::FlatArrayMessageReader fareader(r->table_info_array);
    FosterTableInfo::Reader table_reader = fareader.getRoot<FosterTableInfo>();
    capnp::List<FosterIndexInfo>::Reader indexes = table_reader.getIndexes();
    bool found;
    w_rc_t rc;
    w_keystr_t kstr;
    kstr.construct_regularkey(r->key_buf, r->ksz);
    uchar* buf;
    smsize_t size=1;
    buf= (uchar*) malloc(size);
    rc = foster_handle->find_assoc(indexes[0].getStid(),kstr,buf,size, found);
    if(rc.is_error() && rc.err_num()==eRECWONTFIT){
        buf = (uchar *) realloc(buf, size);
        rc = foster_handle->find_assoc(indexes[0].getStid(),kstr,buf,size, found);
    }
    r->packed_record_buf= buf;
    r->packed_len=size;
    return rc;
}



int fstr_wrk_thr_t::add_to_secondary_idx(StoreID sec_id, w_keystr_t sec_kstr, w_keystr_t primary){

    w_rc_t rc;
    bool found;
    smsize_t size=primary.get_length_as_nonkeystr()+1;
    uint pksz = primary.get_length_as_nonkeystr();
    uchar* record_buf= (uchar*) malloc(size);
    rc=foster_handle->bt->lookup(sec_id, sec_kstr, record_buf, size, found);
    if(rc.is_error() && rc.err_num()==eRECWONTFIT){
        record_buf = (uchar *) realloc(record_buf, size);
        rc=foster_handle->bt->lookup(sec_id, sec_kstr, record_buf, size, found);
    }
    if(found){
        if(foster_handle->bt->max_entry_size()<=size+pksz) {
            uint numberOfRecs = record_buf[0] + 1;
            uchar *new_buf = (uchar *) malloc(numberOfRecs * pksz + 1);
            uchar *curr = new_buf;
            *curr++ = numberOfRecs;
            uchar *tmp_pk_buffer = (uchar *) malloc(pksz);
            w_keystr_t compare_key;
            record_buf++;
            bool done;
            for (uint i = 0; i < numberOfRecs - 1; i++) {
                memcpy(tmp_pk_buffer, record_buf + (i * pksz), pksz);
                compare_key.construct_regularkey(tmp_pk_buffer, pksz);
                if (compare_key.compare(primary) > 0 && !done) {
                    memcpy(curr, primary.serialize_as_nonkeystr().c_str(), pksz);
                    curr += pksz;
                    memcpy(curr, tmp_pk_buffer, pksz);
                    curr += pksz;
                    done = true;
                } else {
                    memcpy(curr, tmp_pk_buffer, pksz);
                    curr += pksz;
                }
            }
            if (!done) {
                memcpy(curr, primary.serialize_as_nonkeystr().c_str(), pksz);
            }
            free(tmp_pk_buffer);
            rc = foster_handle->bt->put(sec_id, sec_kstr, vec_t(new_buf, numberOfRecs * pksz + 1));
            free(new_buf);
            free(--record_buf);
            return 0;
        }else{
            return 1;
        }
    }else{
        *record_buf++=1;
        memcpy(record_buf,primary.serialize_as_nonkeystr().c_str(), primary.get_length_as_nonkeystr());
        record_buf--;
        foster_handle->bt->insert(sec_id, sec_kstr,
                                  vec_t(record_buf, size)) ;
        free(record_buf);
        return 0;
    }
}



int fstr_wrk_thr_t::add_to_secondary_idx_uniquified(StoreID sec_id, w_keystr_t sec_kstr, w_keystr_t primary){
    w_rc_t rc;
    bool found;
    w_keystr_t uniquified;
    foster_handle->bt->max_entry_size();
    uchar* buf = (uchar*) malloc(sec_kstr.get_length_as_keystr()+primary.get_length_as_nonkeystr());
    sec_kstr.serialize_as_nonkeystr(buf);
    primary.serialize_as_nonkeystr(buf+sec_kstr.get_length_as_nonkeystr());
    uniquified.construct_regularkey(buf,sec_kstr.get_length_as_nonkeystr()+primary.get_length_as_nonkeystr());
    foster_handle->bt->insert(sec_id,uniquified,
                              vec_t(primary.serialize_as_nonkeystr().c_str(),primary.get_length_as_nonkeystr()));

}

int fstr_wrk_thr_t::delete_from_secondary_idx(StoreID sec_id, w_keystr_t sec_kstr, w_keystr_t primary){
    w_rc_t rc;
    bool found;
    smsize_t size=primary.get_length_as_nonkeystr()+1;

    uchar* record_buf= (uchar*) malloc(size);

    rc=foster_handle->bt->lookup(sec_id, sec_kstr, record_buf, size, found);
    if(rc.is_error() && rc.err_num()==eRECWONTFIT){
        record_buf = (uchar *) realloc(record_buf, size);
        rc=foster_handle->bt->lookup(sec_id, sec_kstr, record_buf, size, found);
    }
    if(found){
        uint pksz = primary.get_length_as_nonkeystr();
        uint numberOfRecs= record_buf[0]-1;
        if(numberOfRecs==0) {
            rc= foster_handle->bt->remove(sec_id, sec_kstr);
            return 0;
        }

        uchar* new_buf = (uchar*) malloc(numberOfRecs*pksz+1);
        uchar* curr=new_buf;
        *curr++=numberOfRecs;
        uchar* tmp_pk_buffer = (uchar*) malloc(pksz);

        w_keystr_t compare_key;
        record_buf++;
        for(uint i=0; i< numberOfRecs+1; i++){
            memcpy(tmp_pk_buffer, record_buf+(i*pksz), pksz);
            compare_key.construct_regularkey(tmp_pk_buffer, pksz);
            if(compare_key.compare(primary)!=0){
                memcpy(curr, tmp_pk_buffer, pksz);
                curr+=pksz;
            }
        }

        rc=foster_handle->bt->put(sec_id, sec_kstr, vec_t(new_buf, numberOfRecs*pksz+1));
        free(tmp_pk_buffer);
        free(new_buf);
        free(--record_buf);
        return 0;
    }else{
        return 1;
    }

}

int fstr_wrk_thr_t::delete_from_secondary_idx_uniquified(StoreID sec_id,
                                                         w_keystr_t sec_kstr,
                                                         w_keystr_t primary){
    w_rc_t rc;
    w_keystr_t uniquified;
    uchar* buf = (uchar*) malloc(sec_kstr.get_length_as_keystr()+primary.get_length_as_nonkeystr());
    sec_kstr.serialize_as_nonkeystr(buf);
    primary.serialize_as_nonkeystr(buf+sec_kstr.get_length_as_nonkeystr());
    uniquified.construct_regularkey(buf,sec_kstr.get_length_as_nonkeystr()+primary.get_length_as_nonkeystr());
    rc=foster_handle->bt->remove(sec_id,uniquified);
}


w_rc_t fstr_wrk_thr_t::foster_begin(){
    W_COERCE(foster_handle->begin_xct());
    return RCOK;
}

w_rc_t fstr_wrk_thr_t::foster_commit(){
    W_COERCE(foster_handle->commit_xct());
    return RCOK;
}


w_rc_t fstr_wrk_thr_t::foster_rollback(){
    W_COERCE(foster_handle->abort_xct());
    return RCOK;
}