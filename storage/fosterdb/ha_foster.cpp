/* Copyright (c) 2004, 2013, Oracle and/or its affiliates.
   Copyright (c) 2010, 2014, SkySQL Ab.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include <key.h>
#include <table.h>
#include <field.h>
#include <sql_priv.h>
#include <sql_class.h>

#include "ha_foster.h"
#include "fstr_wrk_thr_t.h"


static FOSTER_SHARE *get_share(w_keystr_t table_name);
static int free_share(FOSTER_SHARE *share);
static HASH foster_open_tables;
mysql_mutex_t foster_mutex;

static char* fstr_db_var;
static char* fstr_logdir_var;

static FOSTER_THREAD_POOL *worker_pool;

static handler *foster_create_handler(handlerton *hton,
                                      TABLE_SHARE *table,
                                      MEM_ROOT *mem_root);


fstr_wrk_thr_t* ha_foster::main_thread;


#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key key_mutex_foster_share, key_mutex_foster;

static PSI_mutex_info all_foster_mutexes[]=
        {
                { &key_mutex_foster_share, "foster_share::mutex", 0},
                { &key_mutex_foster, "foster", PSI_FLAG_GLOBAL}
        };




static void init_foster_psi_keys()
{
  const char* category= "foster";
  int count;

  count= array_elements(all_foster_mutexes);
  mysql_mutex_register(category, all_foster_mutexes, count);
}
#endif


static const char *ha_foster_exts[] = {
        NullS
};


std::string
foster_get_sql_stmt(THD* thd,size_t* length)
{
  LEX_STRING* stmt;
  stmt = thd_query_string(thd);
  *length = stmt->length;
  std::string str(stmt->str);
  return(str);
}


search_mode translate_search_mode(ha_rkey_function in){
  switch(in){
    case HA_READ_KEY_EXACT: return KEY_EXACT;
    case HA_READ_KEY_OR_NEXT: return KEY_OR_NEXT;
    case HA_READ_KEY_OR_PREV: return KEY_OR_PREV;
    case HA_READ_AFTER_KEY: return AFTER_KEY;
    case HA_READ_BEFORE_KEY: return BEFORE_KEY;
    case HA_READ_PREFIX: return PREFIX;
    case HA_READ_PREFIX_LAST:
      return PREFIX_LAST;
    case HA_READ_PREFIX_LAST_OR_PREV: return PREFIX_LAST_OR_PREV;
    case HA_READ_MBR_CONTAIN:
      return UNKNOWN;
    case HA_READ_MBR_INTERSECT:
      return UNKNOWN;
    case HA_READ_MBR_DISJOINT:
      return UNKNOWN;
    case HA_READ_MBR_EQUAL:
      return UNKNOWN;
    case HA_READ_MBR_WITHIN:
      return UNKNOWN;
    default:
      return UNKNOWN;
  };
}

int translate_err_code(int err){
  switch(err) {
    case eDUPLICATE:
      return HA_ERR_FOUND_DUPP_KEY;
    case eINTERNAL:
      return HA_ERR_INTERNAL_ERROR;
    case stINTERNAL:
      return HA_ERR_INTERNAL_ERROR;
    case fcINTERNAL:
      return HA_ERR_INTERNAL_ERROR;
    case fcOS:
      return HA_ERR_INTERNAL_ERROR;
    case fcOUTOFMEMORY:
      return HA_ERR_OUT_OF_MEM;
    case eDEADLOCK:
      return HA_ERR_LOCK_DEADLOCK;
    case fcNOTIMPLEMENTED:
      return HA_ERR_NO_REFERENCED_ROW;
    case w_error_ok:
      return 0;
    default: return HA_ERR_INTERNAL_ERROR; break;
  }
}

static uchar* foster_get_key(FOSTER_SHARE *share, size_t *length,
                             my_bool not_used __attribute__((unused)))
{
  *length=share->table_keystr.get_length_as_keystr();
  return (uchar*) share->table_keystr.buffer_as_keystr();
}
static int fstr_commit(handlerton *hton, THD *thd, bool all){
  DBUG_ENTER("foster_commit_func");
  bool multi_stmt = (bool) thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);
  if(multi_stmt || all) {
    fstr_wrk_thr_t *worker = (fstr_wrk_thr_t *) thd_get_ha_data(thd, hton);
    base_request_t *req = new base_request_t();
    //Locking work request mutex
    pthread_mutex_init(&req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
    pthread_mutex_lock(&req->LOCK_work_mutex);
    //Init the request condition -> used to signal the handler when the worker is done
    pthread_cond_init(  &req->COND_work, NULL);

    req->type = FOSTER_COMMIT;
    worker->set_request(req);
    worker->notify(true);

    pthread_mutex_lock(&worker->thread_mutex);
    worker->set_request(req);
    worker->notify(true);
    pthread_cond_signal(&worker->COND_worker);
    pthread_mutex_unlock(&worker->thread_mutex);
    while (!req->notified) {
      pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
    }
    pthread_mutex_unlock(&req->LOCK_work_mutex);


    pthread_mutex_destroy(&req->LOCK_work_mutex);
    delete (req);
    void* null=0;

    thd_set_ha_data(thd, hton, null);
    pthread_mutex_lock(&worker_pool->LOCK_pool_mutex);
    worker_pool->pool.push_back(worker);
    worker_pool->changed=true;
    pthread_cond_broadcast(&worker_pool->COND_pool);
    pthread_mutex_unlock(&worker_pool->LOCK_pool_mutex);
    worker=nullptr;
//    worker->foster_exit();
//    delete (worker);
    DBUG_RETURN(0);
  }else{
    //end of a statement
  }
  DBUG_RETURN(0);
}
static int fstr_rollback(handlerton *hton, THD *thd, bool all){
  DBUG_ENTER("foster_rollback_func");
  //  if(multi_stmt || all) {
  fstr_wrk_thr_t *worker = (fstr_wrk_thr_t *) thd_get_ha_data(thd, hton);
  base_request_t *req = new base_request_t();
  //Locking work request mutex
  pthread_mutex_init(&req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
  pthread_mutex_lock(&req->LOCK_work_mutex);
  //Init the request condition -> used to signal the handler when the worker is done
  pthread_cond_init(  &req->COND_work, NULL);

  req->type = FOSTER_ROLLBACK;
  worker->set_request(req);
  worker->notify(true);

  pthread_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  pthread_cond_signal(&worker->COND_worker);
  pthread_mutex_unlock(&worker->thread_mutex);
  while (!req->notified) {
    pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  pthread_mutex_unlock(&req->LOCK_work_mutex);
  worker->aborted=true;
  pthread_mutex_destroy(&req->LOCK_work_mutex);
  delete (req);

  pthread_mutex_lock(&worker_pool->LOCK_pool_mutex);

  void* null=0;
  thd_set_ha_data(thd, hton, null);
  worker_pool->pool.push_back(worker);
  worker_pool->changed=true;
  pthread_cond_broadcast(&worker_pool->COND_pool);
  pthread_mutex_unlock(&worker_pool->LOCK_pool_mutex);
  worker=nullptr;

  DBUG_RETURN(0);
}
static int fstr_prepare(handlerton *hton, THD *thd, bool all){
  DBUG_ENTER("foster_prepare_func");
  DBUG_RETURN(0);
}
static int foster_init_func(void *p)
{
  DBUG_ENTER("foster_init_func");

  handlerton *foster_hton;
#ifdef HAVE_PSI_INTERFACE
  init_foster_psi_keys();
#endif


  mysql_mutex_init(key_mutex_foster, &foster_mutex, MY_MUTEX_INIT_FAST);
  (void) my_hash_init(&foster_open_tables,system_charset_info,32,0,0,
                      (my_hash_get_key) foster_get_key,0,0);

  foster_hton= (handlerton *)p;
  foster_hton->state=   SHOW_OPTION_YES;
  foster_hton->create=  foster_create_handler;
  foster_hton->flags=   HTON_NO_FLAGS;
  foster_hton->tablefile_extensions= ha_foster_exts;
  foster_hton->commit=fstr_commit;
  foster_hton->rollback=fstr_rollback;
  foster_hton->prepare=fstr_prepare;


  start_stop_request_t* req = new start_stop_request_t();
  //Locking work request mutex
  pthread_mutex_init(&req->LOCK_work_mutex, NULL);
  pthread_mutex_lock(&req->LOCK_work_mutex);
  //Init the request condition -> used to signal the handler when the worker is done
  pthread_cond_init(  &req->COND_work, NULL);
  req->type=FOSTER_STARTUP;
  req->db=fstr_db_var;
  req->logdir=fstr_logdir_var;

  ha_foster::main_thread = new fstr_wrk_thr_t();

  ha_foster::main_thread->translate_err_code= std::bind(translate_err_code,
                                                        std::placeholders::_1);
  pthread_mutex_lock(&ha_foster::main_thread->thread_mutex);
  ha_foster::main_thread->set_request(req);
  ha_foster::main_thread->notify(true);
  pthread_cond_signal(&ha_foster::main_thread->COND_worker);
  pthread_mutex_unlock(&ha_foster::main_thread->thread_mutex);

  while(!req->notified){
    pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  pthread_mutex_unlock(&req->LOCK_work_mutex);
  pthread_mutex_destroy(&req->LOCK_work_mutex);
  delete(req);

  worker_pool= new FOSTER_THREAD_POOL;
  pthread_mutex_init(&worker_pool->LOCK_pool_mutex, NULL);
  pthread_cond_init(&worker_pool->COND_pool, NULL);
  pthread_mutex_lock(&worker_pool->LOCK_pool_mutex);
  worker_pool->changed=false;
  worker_pool->pool=*new std::vector<fstr_wrk_thr_t*>();
  for(int i=0; i< worker_pool->pool_size; i++){
    fstr_wrk_thr_t* curr = new fstr_wrk_thr_t();
    curr->translate_err_code= std::bind(translate_err_code, std::placeholders::_1);
    worker_pool->pool.push_back(curr);
  }
  pthread_mutex_unlock(&worker_pool->LOCK_pool_mutex);
  DBUG_RETURN(0);
}


static int foster_deinit_func(void *p){
  DBUG_ENTER("foster_deinit_func");

  pthread_mutex_init(&worker_pool->LOCK_pool_mutex, NULL);
  pthread_cond_init(&worker_pool->COND_pool, NULL);
  pthread_mutex_lock(&worker_pool->LOCK_pool_mutex);
  worker_pool->changed=false;
  for(uint i=0; i< worker_pool->pool_size; i++){
    worker_pool->pool.at(i)->foster_exit();
    delete(worker_pool->pool.at(i));
  }
  pthread_mutex_unlock(&worker_pool->LOCK_pool_mutex);
  pthread_mutex_destroy(&worker_pool->LOCK_pool_mutex);
  pthread_cond_destroy(&worker_pool->COND_pool);
  delete(worker_pool);


  start_stop_request_t* req = new start_stop_request_t();

  //Locking work request mutex
  pthread_mutex_init(&req->LOCK_work_mutex, NULL);
  pthread_mutex_lock(&req->LOCK_work_mutex);
  //Init the request condition -> used to signal the handler when the worker is done
  pthread_cond_init(  &req->COND_work, NULL);

  req->type=FOSTER_SHUTDOWN;

  pthread_mutex_lock(&ha_foster::main_thread->thread_mutex);
  ha_foster::main_thread->set_request(req);
  ha_foster::main_thread->notify(true);
  pthread_cond_broadcast(&ha_foster::main_thread->COND_worker);
  pthread_mutex_unlock(&ha_foster::main_thread->thread_mutex);

  while(!req->notified){
    pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  pthread_mutex_unlock(&req->LOCK_work_mutex);
  delete(req);
  ha_foster::main_thread->foster_exit();
  delete(ha_foster::main_thread);

  my_hash_free(&foster_open_tables);
  mysql_mutex_destroy(&foster_mutex);
  DBUG_RETURN(0);
}




static FOSTER_SHARE *get_share(w_keystr_t table_name)
{
  FOSTER_SHARE *share;
  uint length;
  mysql_mutex_lock(&foster_mutex);
  length=table_name.get_length_as_keystr();

  /*
    If share is not present in the hash, create a new share and
    initialize its members.
  */
  if (!(share=(FOSTER_SHARE*) my_hash_search(&foster_open_tables,
                                             (uchar*) table_name.buffer_as_keystr(),
                                             length)))
  {
    if (!my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
                         &share, sizeof(*share),
                         NullS))
    {
      mysql_mutex_unlock(&foster_mutex);
      return NULL;
    }

    share->use_count= 0;
    share->table_keystr=table_name;
    share->table_name_length= length;

    if (my_hash_insert(&foster_open_tables, (uchar*) share))
      goto error;
    thr_lock_init(&share->share_lock);
    mysql_mutex_init(key_mutex_foster_share,
                     &share->share_mutex, MY_MUTEX_INIT_FAST);
  }

  share->use_count++;
  mysql_mutex_unlock(&foster_mutex);

  return share;

  error:
  mysql_mutex_unlock(&foster_mutex);
  my_free(share);

  return NULL;
}



/*
  Free lock controls.
*/
static int free_share(FOSTER_SHARE *share)
{
  DBUG_ENTER("foster::free_share");
  mysql_mutex_lock(&foster_mutex);
  int result_code= 0;
  if (!--share->use_count){
    my_hash_delete(&foster_open_tables, (uchar*) share);
    thr_lock_delete(&share->share_lock);
    mysql_mutex_destroy(&share->share_mutex);
    my_free(share);
  }
  mysql_mutex_unlock(&foster_mutex);

  DBUG_RETURN(result_code);
}

static handler* foster_create_handler(handlerton *hton,
                                      TABLE_SHARE *table,
                                      MEM_ROOT *mem_root)
{
  return new (mem_root) ha_foster(hton, table);
}



ha_foster::ha_foster(handlerton *hton, TABLE_SHARE *table_arg)
        :handler(hton, table_arg)
{
}



int ha_foster::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_foster::open");

  w_keystr_t normalized =construct_cat_key(table->s->db.str,table->s->table_name.str);
  if (!(share= get_share(normalized))){
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }

  table_name = name;
  ref_length = table->key_info[0].key_length;

  for(uint i=0; i<table->s->keys; i++) {

    if (max_key_name_len < table->key_info[i].name_length)
      max_key_len = table->key_info[i].name_length;

  }
  max_key_len = max_key_len+table->s->keys*2;
  if(!share->initialized){
    discovery_request_t* req = new discovery_request_t();
    req->type=FOSTER_DISCOVERY;
    pthread_mutex_init(&req->LOCK_work_mutex, NULL);
    //Init the request condition -> used to signal the handler when the worker is done
    pthread_cond_init(  &req->COND_work, NULL);
    pthread_mutex_lock(&req->LOCK_work_mutex);


    req->table_name= *new string(table->s->table_name.str);
    req->db_name=*new string(table->s->db.str);

    pthread_mutex_lock(&worker_pool->LOCK_pool_mutex);
    while(worker_pool->pool.empty() && !worker_pool->changed){
      pthread_cond_wait(&worker_pool->COND_pool, &worker_pool->LOCK_pool_mutex);
    }
    fstr_wrk_thr_t* func_worker =worker_pool->pool.back();
    worker_pool->pool.pop_back();
    worker_pool->changed=false;
    pthread_mutex_unlock(&worker_pool->LOCK_pool_mutex);

    pthread_mutex_lock(&func_worker->thread_mutex);
    func_worker->set_request(req);
    func_worker->notify(true);
    pthread_cond_broadcast(&func_worker->COND_worker);
    pthread_mutex_unlock(&func_worker->thread_mutex);

    while(!req->notified){
      pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
    }
    pthread_mutex_unlock(&req->LOCK_work_mutex);

    pthread_mutex_lock(&worker_pool->LOCK_pool_mutex);
    worker_pool->pool.push_back(func_worker);
    worker_pool->changed=true;
    pthread_cond_broadcast(&worker_pool->COND_pool);
    pthread_mutex_unlock(&worker_pool->LOCK_pool_mutex);

    share->table_info_array=req->table_info_array;
    share->initialized=true;
    pthread_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
  }

  key_buffer = (uchar*) my_malloc(max_key_len, MYF(MY_WME));
  if (key_buffer == 0)
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  record_buffer= create_record_buffer(table->s->reclength);
  if (!record_buffer)
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  thr_lock_data_init(&share->share_lock,&lock,(void*) this);
  DBUG_RETURN(0);
}


int ha_foster::close(void)
{
  int rc= 0;
  DBUG_ENTER("ha_foster::close");
  destroy_record_buffer(record_buffer);
  //if(key_buffer) free(key_buffer);
  DBUG_RETURN(free_share(share) || rc);
}


std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
  return elems;
}

std::vector<std::string> split(const std::string &s, char delim) {
  std::vector<std::string> elems;
  split(s, delim, elems);
  return elems;
}

string normalize_table_name(const char* table_name){
  string normalized;
  for(int i=strlen(table_name); i<0; i--){
    if(table_name[i]=='.') continue;
    if(table_name[i]=='/') break;
    normalized.push_back(table_name[i]);
  }
  return normalized;
}

/**
 * Memory -> Disk
 */
uint pack_row(uchar *from, uchar* buffer, TABLE* table)
{
    TABLE* curr_table = (TABLE*) table;
    uchar* ptr;
    memcpy(buffer, from, curr_table->s->null_bytes);
    ptr= buffer + curr_table->s->null_bytes;

    for (Field **field=curr_table->field ; *field ; field++)
    {
        if (!((*field)->is_null()))
            ptr= (*field)->pack(ptr, from + (*field)->offset(from));
    }
    return (unsigned int) (ptr - buffer);
}

/**
 * Disk->Memory
 */
int unpack_row(uchar *record, int row_len, uchar *&to, TABLE* table)
{
    TABLE* curr_table = (TABLE*) table;
    /* Copy null bits */
    const uchar* end= record+ row_len;
    memcpy(to, record, curr_table->s->null_bytes);
    record+=curr_table->s->null_bytes;
    if (record > end)
        return HA_ERR_WRONG_IN_RECORD;
    for (Field **field=curr_table->field ; *field ; field++)
    {
        if (!((*field)->is_null_in_record(to)))
        {
            if (!(record= const_cast<uchar*>((*field)
                    ->unpack(to + (*field)->offset(curr_table->record[0]),record, end))))
                return HA_ERR_WRONG_IN_RECORD;
        }
    }
    if (record != end)
        return HA_ERR_WRONG_IN_RECORD;
    return 0;
}
int ha_foster::create(const char *name, TABLE *table_arg,
                      HA_CREATE_INFO *create_info)
{
  int rc=0;
  DBUG_ENTER("ha_foster::create");
  ddl_request_t* req = new ddl_request_t();

  req->capnpTable = req->message.initRoot<FosterTableInfo>();
  //Locking work request mutex
  pthread_mutex_init(&req->LOCK_work_mutex, NULL);
  pthread_mutex_lock(&req->LOCK_work_mutex);
  req->foreign_keys = *new vector<FOSTER_FK_INFO>();
  req->capnpTable.setTablename(table_arg->s->table_name.str);
  req->capnpTable.setDbname(table_arg->s->db.str);
  ::capnp::List<FosterIndexInfo>::Builder indexes = req->capnpTable .initIndexes(table_arg->s->keys);
  for(uint i=0; i<table_arg->s->keys; i++){
    FosterIndexInfo::Builder curr_index = indexes[i];
    curr_index.setIndexname(table_arg->key_info[i].name);
    curr_index.setKeylength(table_arg->key_info[i].key_length);
    ::capnp::List<FosterFieldInfo>::Builder partInfo = curr_index.initPartinfo(
            table_arg->key_info[i].usable_key_parts);
    for(uint j=0; j< table_arg->key_info[i].usable_key_parts; j++){
      FosterFieldInfo::Builder curr_part = partInfo[j];
      curr_part.setFieldname(table_arg->key_info[i].key_part[j].field->field_name);
      table_arg->key_info[i].key_part[j].field->set_default();
      curr_part.setOffset(table_arg->key_info[i].key_part[j].offset);
      curr_part.setLength(table_arg->key_info[i].key_part[j].length);
      curr_part.setNullBit(table_arg->key_info[i].key_part[j].null_bit);
      curr_part.setNullOffset(table_arg->key_info[i].key_part[j].null_offset);
      curr_part.setBlob((table_arg->key_info[i].key_part[j].key_part_flag & HA_BLOB_PART) != 0);
      curr_part.setVarlength((table_arg->key_info[i].key_part[j].key_part_flag & HA_VAR_LENGTH_PART) != 0);
    }
  }
  size_t length;
  string sql_stmt= foster_get_sql_stmt(ha_thd(), &length);
  string foreignKey("FOREIGN KEY");

  size_t found;
  int index = 0;
  found = sql_stmt.find(foreignKey);
  string curr_part = sql_stmt;
  while(found!=std::string::npos) {
    curr_part=curr_part.substr(found+11);
    index++;
    FOSTER_FK_INFO* fk_info = new FOSTER_FK_INFO();
    string* foreign_id = new string();
    foreign_id->append("fk::");
    foreign_id->append(table_arg->s->table_name.str);
    foreign_id->append("::");
    foreign_id->append(to_string(index));
    fk_info->foreign_id=*foreign_id;
    fk_info->referenced_db= *new string(table_arg->s->db.str);
    fk_info->foreign_table= *new string(table_arg->s->table_name.str);
    string referenceStr= "REFERENCES";
    size_t referencePos = curr_part.find(referenceStr);
    string ondeleteStr="ON DELETE";
    size_t ondeletePos= curr_part.find(ondeleteStr);
    string onUpdateStr="ON UPDATE";
    size_t onUpdatePos= curr_part.find(onUpdateStr);

    string refPart;
    if(ondeletePos<onUpdatePos){
      refPart= curr_part.substr(referencePos+11, ondeletePos-(referencePos+11));
    }else{
      refPart = curr_part.substr(referencePos, onUpdatePos-(referencePos+11));
    }
    //Extract foreign fields
    size_t bracketpos=curr_part.find('(');
    string forKeyStr = curr_part.substr(bracketpos+1, curr_part.find(')')-bracketpos);
    fk_info->foreign_fields = *new vector<string>();
    std::vector<std::string> fk_columns = split(forKeyStr, ',');
    std::vector<string>::iterator fk_columns_it;
    for(fk_columns_it=fk_columns.begin() ; fk_columns_it< fk_columns.end(); fk_columns_it++ ) {
      string column;
      for(std::string::size_type i = 0; i < fk_columns_it->size(); ++i) {
        if(fk_columns_it->at(i)==' ' || fk_columns_it->at(i)==')') continue;
        column.push_back(fk_columns_it->at(i));
      }
      fk_info->foreign_fields.push_back(column);
    }

    bracketpos=refPart.find('(');
    fk_info->referenced_table = *new string();
    fk_info->referenced_table.assign(refPart.substr(0, bracketpos));

    string refColStr = refPart.substr(bracketpos+1, refPart.find(')')-(bracketpos));
    std::vector<std::string> ref_columns = split(refColStr, ',');
    fk_info->referenced_fields = *new std::vector<string>();
    vector<string>::iterator ref_columns_it;
    for(ref_columns_it=ref_columns.begin(); ref_columns_it< ref_columns.end(); ref_columns_it++ ) {
      string column;
      for(std::string::size_type i = 0; i < ref_columns_it->size(); ++i) {
        if(ref_columns_it->at(i)==' ' || ref_columns_it->at(i)==')') continue;
        column.push_back(ref_columns_it->at(i));
      }
      fk_info->referenced_fields.push_back(column);
    }
    if(ondeletePos!=std::string::npos){
      string mode = curr_part.substr(ondeletePos, 20);
      if(mode.find("CASCADE")!=std::string::npos) fk_info->delete_method=1;
      if(mode.find("RESTRICT")!=std::string::npos) fk_info->delete_method=2;
      if(mode.find("SET NULL")!=std::string::npos) fk_info->delete_method=3;
      if(mode.find("NO ACTION")!=std::string::npos) fk_info->delete_method=4;
    }
    if(onUpdatePos!=std::string::npos){
      string mode = curr_part.substr(onUpdatePos, 20);
      if(mode.find("CASCADE")!=std::string::npos) fk_info->update_method=1;
      if(mode.find("RESTRICT")!=std::string::npos) fk_info->update_method=2;
      if(mode.find("SET NULL")!=std::string::npos) fk_info->update_method=3;
      if(mode.find("NO ACTION")!=std::string::npos) fk_info->update_method=4;
    }
    req->foreign_keys.push_back(*fk_info);
    curr_part= curr_part.substr(ondeletePos>onUpdatePos?ondeletePos+20:onUpdatePos+20);
    found = curr_part.find(foreignKey);
  }


  //Init the request condition -> used to signal the handler when the worker is done
  pthread_cond_init(  &req->COND_work, NULL);
  req->type=FOSTER_CREATE;

  pthread_mutex_lock(&worker_pool->LOCK_pool_mutex);
  while(worker_pool->pool.empty() && !worker_pool->changed){
    pthread_cond_wait(&worker_pool->COND_pool, &worker_pool->LOCK_pool_mutex);
  }
  fstr_wrk_thr_t* func_worker =worker_pool->pool.back();
  worker_pool->pool.pop_back();
  worker_pool->changed=false;
  pthread_mutex_unlock(&worker_pool->LOCK_pool_mutex);

  func_worker->translate_err_code= std::bind(translate_err_code, std::placeholders::_1);
  pthread_mutex_lock(&func_worker->thread_mutex);
  func_worker->set_request(req);
  func_worker->notify(true);
  pthread_cond_broadcast(&func_worker->COND_worker);
  pthread_mutex_unlock(&func_worker->thread_mutex);

  while(!req->notified){
    pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }

  for(int i=0; i<req->foreign_keys.size(); i++){
    w_keystr_t ref_cat =
            construct_cat_key(req->foreign_keys.at(i).referenced_db,
                              req->foreign_keys.at(i).referenced_table);
    FOSTER_SHARE* tmp;
    if((tmp =(FOSTER_SHARE*) my_hash_search(&foster_open_tables,
                                            (uchar*) ref_cat.buffer_as_keystr(),
                                            ref_cat.get_length_as_keystr()))){
      mysql_mutex_lock(&tmp->share_mutex);
      tmp->table_info_array= req->foreign_keys.at(i).reftables;
      mysql_mutex_unlock(&tmp->share_mutex);
    }
  }
  pthread_mutex_unlock(&req->LOCK_work_mutex);
  pthread_mutex_destroy(&req->LOCK_work_mutex);


  pthread_mutex_lock(&worker_pool->LOCK_pool_mutex);
  worker_pool->pool.push_back(func_worker);
  worker_pool->changed=true;
  pthread_cond_broadcast(&worker_pool->COND_pool);
  pthread_mutex_unlock(&worker_pool->LOCK_pool_mutex);

  delete(req);
  DBUG_RETURN(rc);
}



int ha_foster::delete_table(const char *name)
{
  int rc=0;
  DBUG_ENTER("ha_foster::delete_table");
  ddl_request_t* req = new ddl_request_t();
  pthread_mutex_init(&req->LOCK_work_mutex, NULL);
  pthread_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  pthread_cond_init(  &req->COND_work, NULL);

  pthread_mutex_lock(&worker_pool->LOCK_pool_mutex);
  while(worker_pool->pool.empty() && !worker_pool->changed){
    pthread_cond_wait(&worker_pool->COND_pool, &worker_pool->LOCK_pool_mutex);
  }
  fstr_wrk_thr_t* del_worker =worker_pool->pool.back();
  worker_pool->pool.pop_back();
  worker_pool->changed=false;
  pthread_mutex_unlock(&worker_pool->LOCK_pool_mutex);

  del_worker->translate_err_code= std::bind(translate_err_code, std::placeholders::_1);
  string separator= "###";
  req->type=FOSTER_DELETE;
  req->table_name= *new string();
  req->db_name =*new string();
  bool first_slash= false;
  for(uint i=0; i<strlen(name); i++){
    if(name[i]=='.') continue;
    if(name[i]=='/'){
      first_slash=!first_slash;
      continue;
    }
    if(first_slash){
      req->db_name.push_back(name[i]);
    }else{
      req->table_name.push_back(name[i]);
    }

  }

  pthread_mutex_lock(&del_worker->thread_mutex);
  del_worker->set_request(req);
  del_worker->notify(true);
  pthread_cond_broadcast(&del_worker ->COND_worker);
  pthread_mutex_unlock(&del_worker->thread_mutex);

  while(!req->notified){
    pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  rc=req->err;
  pthread_mutex_unlock(&req->LOCK_work_mutex);
  pthread_mutex_destroy(&req->LOCK_work_mutex);
  delete(req);

  pthread_mutex_lock(&worker_pool->LOCK_pool_mutex);
  worker_pool->pool.push_back(del_worker);
  worker_pool->changed=true;
  pthread_cond_broadcast(&worker_pool->COND_pool);
  pthread_mutex_unlock(&worker_pool->LOCK_pool_mutex);

  DBUG_RETURN(rc);
}

int ha_foster::write_row(uchar *buf) {
  int rc=0;
  DBUG_ENTER("ha_foster::write_row");
  write_request_t *req = new write_request_t();
  //Locking work request mutex
  pthread_mutex_init(&req->LOCK_work_mutex, NULL);
  pthread_mutex_lock(&req->LOCK_work_mutex);
  //Init the request condition -> used to signal the handler when the worker is done
  pthread_cond_init(  &req->COND_work, NULL);


  //Initializing values of the request
  req->type = FOSTER_WRITE_ROW;
  req->key_buf=key_buffer;

  req->mysql_format_buf=buf;
  req->table_info_array=share->table_info_array;
  if (fix_rec_buff(max_row_length(buf)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  req->packed_record_buf=record_buffer->buffer;

  req->packed_len= pack_row( buf, req->packed_record_buf,table);


  req->max_key_len=max_key_len;

  pthread_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  pthread_cond_broadcast(&worker->COND_worker);
  pthread_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  pthread_mutex_unlock(&req->LOCK_work_mutex);
  rc=req->err;
  pthread_mutex_destroy(&req->LOCK_work_mutex);
  delete(req);
  DBUG_RETURN(rc);
}


int ha_foster::update_row(const uchar *old_data, uchar *new_data)
{
  int rc=0;
  DBUG_ENTER("ha_foster::update_row");
  write_request_t *req = new write_request_t();

  //Locking work request mutex
  pthread_mutex_init(&req->LOCK_work_mutex, NULL);
  pthread_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  pthread_cond_init(  &req->COND_work, NULL);

  req->type = FOSTER_UPDATE_ROW;
  req->key_buf=key_buffer;

  req->mysql_format_buf=new_data;
  req->old_mysql_format_buf= const_cast<uchar*>(old_data);
  req->max_key_len=max_key_len;

  if (fix_rec_buff(max_row_length(new_data)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  req->packed_record_buf=record_buffer->buffer;

  req->packed_len= pack_row( new_data, req->packed_record_buf,table);

  req->table_info_array=share->table_info_array;
  pthread_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  pthread_cond_broadcast(&worker->COND_worker);
  pthread_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }

  rc=req->err;

  pthread_mutex_unlock(&req->LOCK_work_mutex);

  pthread_mutex_destroy(&req->LOCK_work_mutex);
  delete(req);
  DBUG_RETURN(rc);
}


int ha_foster::delete_row(const uchar *buf)
{
  int rc=0;
  DBUG_ENTER("ha_foster::delete_row");
  write_request_t *req = new write_request_t();
  //Locking work request mutex
  pthread_mutex_init(&req->LOCK_work_mutex, NULL);
  pthread_mutex_lock(&req->LOCK_work_mutex);
  //Init the request condition -> used to signal the handler when the worker is done
  pthread_cond_init(  &req->COND_work, NULL);

  req->type = FOSTER_DELETE_ROW;
  req->key_buf=key_buffer;
  req->mysql_format_buf=const_cast<uchar*>(buf);
//  req->table_info = &share->info;
  req->max_key_len=max_key_len;
  req->table_info_array=share->table_info_array;

  pthread_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  pthread_cond_broadcast(&worker->COND_worker);
  pthread_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  pthread_mutex_unlock(&req->LOCK_work_mutex);

  if(req->err==0){
    pthread_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status=0;
    rc= 0;
  }else{

    pthread_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    rc=HA_ERR_END_OF_FILE;
  }
  DBUG_RETURN(rc);
}
int ha_foster::index_init(uint idx, bool sorted){
  active_index=idx;
  return 0;
}
int ha_foster::index_end(){
  return 0;
}
int ha_foster::index_read_idx_map(uchar * buf, uint index, const uchar * key,
                                  key_part_map keypart_map,
                                  enum ha_rkey_function find_flag){
  int rc=0;
  DBUG_ENTER("ha_foster::index_read_idx_map");
  read_request_t *req = new read_request_t();

  table->status = 0;
  //Locking work request mutex
  pthread_mutex_init( &req->LOCK_work_mutex, NULL);
  pthread_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  pthread_cond_init(  &req->COND_work, NULL);

  req->type = FOSTER_IDX_READ;
  req->key_buf = const_cast<uchar*>(key);
  req->ksz=table->s->key_info[index].key_length;
  req->find_flag=translate_search_mode(find_flag);

  req->idx_no=index;
  req->table_info_array=share->table_info_array;
  req->idx_no = index;

  pthread_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  pthread_cond_broadcast(&worker->COND_worker);
  pthread_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  pthread_mutex_unlock(&req->LOCK_work_mutex);

  if(req->err==0){
    unpack_row(req->packed_record_buf,req->packed_len, buf,table);
    rc=0;
    pthread_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status=0;
  }else{
    pthread_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status = STATUS_NOT_FOUND;
    rc=HA_ERR_END_OF_FILE;
  }
  DBUG_RETURN(rc);
}

int ha_foster::index_read(uchar *buf, const uchar *key, uint key_len,
                          enum ha_rkey_function find_flag){
  int rc=0;
  DBUG_ENTER("ha_foster::index_read");
  read_request_t *req = new read_request_t();

  table->status = 0;
  //Locking work request mutex
  pthread_mutex_init(&req->LOCK_work_mutex, NULL);
  pthread_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  pthread_cond_init(  &req->COND_work, NULL);

  req->type = FOSTER_IDX_READ;
  req->key_buf = const_cast<uchar*>(key);
  req->ksz=key_len;
  req->find_flag=translate_search_mode(find_flag);
  req->idx_no=active_index;

  req->table_info_array=share->table_info_array;

  pthread_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  pthread_cond_broadcast(&worker->COND_worker);
  pthread_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  pthread_mutex_unlock(&req->LOCK_work_mutex);
  rc=req->err;
  if(req->err==0){
    unpack_row(req->packed_record_buf,req->packed_len, buf,table);
    pthread_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status=0;
  }else{
    pthread_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status = STATUS_NOT_FOUND;
    rc=HA_ERR_END_OF_FILE;
  }
  DBUG_RETURN(rc);
}

int ha_foster::index_next(uchar *buf)
{
  int rc=0;
  DBUG_ENTER("ha_foster::index_next");
  read_request_t *req = new read_request_t();
  table->status = 0;
  //Locking work request mutex
  pthread_mutex_init(&req->LOCK_work_mutex, NULL);
  pthread_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  pthread_cond_init(  &req->COND_work, NULL);
  req->type = FOSTER_IDX_NEXT;
  req->idx_no=active_index;

  req->table_info_array=share->table_info_array;

  pthread_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  pthread_cond_broadcast(&worker->COND_worker);
  pthread_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  pthread_mutex_unlock(&req->LOCK_work_mutex);

  rc=req->err;
  if(req->err==0){
    unpack_row(req->packed_record_buf,req->packed_len, buf,table);
    pthread_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status=0;
  }else{
    pthread_mutex_destroy(&req->LOCK_work_mutex);
    table->status = STATUS_NOT_FOUND;
    delete(req);
    rc= HA_ERR_END_OF_FILE;
  }
  DBUG_RETURN(rc);
}


//int ha_foster::index_prev(uchar *buf)
//{
//  DBUG_RETURN(HA_ERR_WRONG_IN_RECORD);
//}


int ha_foster::index_first(uchar *buf)
{

  int rc =0;
  DBUG_ENTER("ha_foster::index_first");

  rc= index_read(buf, 0, 0, HA_READ_KEY_OR_NEXT);
  DBUG_RETURN(rc);
}


int ha_foster::index_last(uchar *buf)
{
  int rc =0;
  DBUG_ENTER("ha_foster::index_last");

  DBUG_RETURN(index_read(buf, 0, 0, HA_READ_PREFIX_LAST));

  DBUG_RETURN(rc);
}



int ha_foster::rnd_init(bool scan)
{
  DBUG_ENTER("ha_foster::rnd_init");

  first_row=true;
  active_index=0;
  DBUG_RETURN(0);
}

int ha_foster::rnd_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_foster::rnd_next");

  table->status = 0;
  read_request_t *req = new read_request_t();

  pthread_mutex_init(&req->LOCK_work_mutex, NULL);
  pthread_mutex_lock(&req->LOCK_work_mutex);

  pthread_cond_init(  &req->COND_work, NULL);
  req->idx_no=0;
  req->table_info_array=share->table_info_array;

  if(first_row){
    first_row=false;
    req->key_buf =0;
    req->ksz=0;
    req->find_flag=KEY_OR_NEXT;
    req->type = FOSTER_IDX_READ;
  }else{
    req->type = FOSTER_IDX_NEXT;
  }

  pthread_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  pthread_cond_broadcast(&worker->COND_worker);
  pthread_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  pthread_mutex_unlock(&req->LOCK_work_mutex);
  rc=req->err;

  if(req->err==0){
    unpack_row(req->packed_record_buf,req->packed_len, buf,table);
    pthread_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status=0;
  }else{
    pthread_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status = STATUS_NOT_FOUND;
    rc=HA_ERR_END_OF_FILE;
  }
  DBUG_RETURN(rc);
}



void ha_foster::position(const uchar *record)
{
  DBUG_ENTER("ha_foster::position");

  key_copy(ref, const_cast<uchar*>(record),
           &table->key_info[0], 0);
  DBUG_VOID_RETURN;


}


int ha_foster::rnd_pos(uchar *buf, uchar *pos)
{
  int rc=0;
  DBUG_ENTER("ha_foster::rnd_pos");
  read_request_t *req = new read_request_t();

  table->status = 0;
  //Locking work request mutex
  pthread_mutex_init(&req->LOCK_work_mutex, NULL);
  pthread_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  pthread_cond_init(  &req->COND_work, NULL);

//  req->mysql_format_buf=buf;
  req->key_buf=pos;
  req->ksz=ref_length;
  req->idx_no=0;
  req->type=FOSTER_POS_READ;

  req->table_info_array=share->table_info_array;
  pthread_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  pthread_cond_broadcast(&worker->COND_worker);
  pthread_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  pthread_mutex_unlock(&req->LOCK_work_mutex);

  rc=req->err;
  if(req->err==0){
    unpack_row(req->packed_record_buf,req->packed_len, buf,table);
    pthread_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status=0;
  }else{
    pthread_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status = STATUS_NOT_FOUND;
    rc=HA_ERR_END_OF_FILE;
  }
  DBUG_RETURN(rc);
}


int ha_foster::info(uint flag)
{
  DBUG_ENTER("ha_foster::info");
  DBUG_RETURN(0);
}


int ha_foster::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("ha_foster::extra");
  DBUG_RETURN(0);
}



int ha_foster::delete_all_rows()
{
  DBUG_ENTER("ha_foster::delete_all_rows");
  DBUG_RETURN(0);
}



int ha_foster::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_foster::external_lock");
  bool multi_stmt= (bool) thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);
  worker = (fstr_wrk_thr_t*) thd_get_ha_data(thd, ht);
  if (lock_type == F_UNLCK)
  {
    worker->numUsedTables--;
    if (!worker->numUsedTables)
    {
      // end of statement
      if (!multi_stmt) {
        if(!worker->aborted) {
          base_request_t *req = new base_request_t();
          //Locking work request mutex
          pthread_mutex_init(&req->LOCK_work_mutex, NULL);
          pthread_mutex_lock(&req->LOCK_work_mutex);
          //Init the request condition -> used to signal the handler when the worker is done
          pthread_cond_init(&req->COND_work, NULL);

          req->type = FOSTER_COMMIT;
          worker->set_request(req);
          worker->notify(true);
          pthread_mutex_lock(&worker->thread_mutex);
          pthread_cond_signal(&worker->COND_worker);
          pthread_mutex_unlock(&worker->thread_mutex);
          while (!req->notified) {
            pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
          }
          pthread_mutex_unlock(&req->LOCK_work_mutex);
          delete (req);
        }
        void* null=0;
        thd_set_ha_data(thd, ht, null);
        pthread_mutex_lock(&worker_pool->LOCK_pool_mutex);
        worker_pool->pool.push_back(worker);
        worker_pool->changed=true;
        pthread_cond_broadcast(&worker_pool->COND_pool);
        pthread_mutex_unlock(&worker_pool->LOCK_pool_mutex);
//        worker->foster_exit();
//        delete (worker);
      }
    }
  }
  else
  {
    if (!worker)
    {
      pthread_mutex_lock(&worker_pool->LOCK_pool_mutex);
      // beginning of new transaction
//      worker = new fstr_wrk_thr_t(true);
//      worker->translate_err_code =std::bind(translate_err_code, std::placeholders::_1);
      while(worker_pool->pool.empty() && !worker_pool->changed){
        pthread_cond_wait(&worker_pool->COND_pool, &worker_pool->LOCK_pool_mutex);
      }


      worker=worker_pool->pool.back();
      worker_pool->pool.pop_back();
      worker_pool->changed=false;
      worker->numUsedTables=1;
      base_request_t *req = new base_request_t();
      //Locking work request mutex
      pthread_mutex_init(&req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
      pthread_mutex_lock(&req->LOCK_work_mutex);
      //Init the request condition -> used to signal the handler when the worker is done
      pthread_cond_init(  &req->COND_work, NULL);

      req->type = FOSTER_BEGIN;
      worker->set_request(req);
      worker->notify(true);
      pthread_mutex_lock(&worker->thread_mutex);
      worker->set_request(req);
      worker->notify(true);
      pthread_cond_signal(&worker->COND_worker);
      pthread_mutex_unlock(&worker->thread_mutex);
      while (!req->notified) {
        pthread_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
      }
      pthread_mutex_unlock(&req->LOCK_work_mutex);

      pthread_mutex_destroy(&req->LOCK_work_mutex);
      delete (req);

      pthread_mutex_unlock(&worker_pool->LOCK_pool_mutex);

      thd_set_ha_data(thd, ht, worker);
      trans_register_ha(thd, multi_stmt, ht);

    }else {
      worker->numUsedTables++;
    }
  }
  DBUG_RETURN(0);
}



THR_LOCK_DATA ** ha_foster::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lockType)
{
  if (lockType != TL_IGNORE && lock.type == TL_UNLOCK)
  {
    // allow concurrent write operations on table
    if (lockType >= TL_WRITE_CONCURRENT_INSERT && lockType <= TL_WRITE)
    {
      if (!thd_in_lock_tables(thd) && !thd_tablespace_op(thd))
        lockType = TL_WRITE_ALLOW_WRITE;
    }
      // allow write operations on tables currently read from
    else if (lockType == TL_READ_NO_INSERT && !thd_in_lock_tables(thd))
      lockType = TL_READ;
    lock.type = lockType;
  }

  *to++ = &lock;
  return to;
}





ha_rows ha_foster::records_in_range(uint inx, key_range *min_key,
                                    key_range *max_key)
{
  DBUG_ENTER("ha_foster::records_in_range");
  DBUG_RETURN(10);                         // low number to force index usage
}


void ha_foster::destroy_record_buffer(foster_record_buffer *r)
{
  DBUG_ENTER("ha_foster::destroy_record_buffer");
  my_free(r->buffer);
  delete(r);
  DBUG_VOID_RETURN;
}

/*
  Calculate max length needed for row. This includes
  the bytes required for the length in the header.
*/

uint32 ha_foster::max_row_length(const uchar *buf)
{
  uint32 length= (uint32) table->s->reclength + table->s->fields*2;

  uint *ptr, *end;
  for (ptr= table->s->blob_field, end=ptr + table->s->blob_fields ;
       ptr != end ;
       ptr++)
  {
    if (!table->field[*ptr]->is_null())
      length += 2 + ((Field_blob*)table->field[*ptr])->get_length();
  }

  return length;
}



/* Reallocate buffer if needed */
bool ha_foster::fix_rec_buff(unsigned int length) {
  DBUG_ENTER("ha_foster::fix_rec_buff");
  DBUG_PRINT("ha_foster", ("Fixing %u for %u",
          length, record_buffer->length));
  DBUG_ASSERT(record_buffer->buffer);

  if (length > record_buffer->length) {
    uchar *newptr;
    if (!(newptr = (uchar *) my_realloc(record_buffer->buffer,
                                        length,
                                        MYF(MY_ALLOW_ZERO_PTR))))
      DBUG_RETURN(1);
    record_buffer->buffer = newptr;
    record_buffer->length = length;
  }

  DBUG_ASSERT(length <= record_buffer->length);
  DBUG_RETURN(0);
}



foster_record_buffer *ha_foster::create_record_buffer(ulong length)
{
  DBUG_ENTER("ha_foster::create_record_buffer");
  foster_record_buffer *r = new foster_record_buffer();
  r->length= (int)length;
  if (!(r->buffer= (uchar*) my_malloc(r->length,
                                      MYF(MY_WME))))
  {
    delete(r);
    DBUG_RETURN(NULL);
  }

  DBUG_RETURN(r);
}


enum_alter_inplace_result
ha_foster::check_if_supported_inplace_alter(TABLE* altered_table,
                                            Alter_inplace_info* ha_alter_info)
{
  HA_CREATE_INFO *info= ha_alter_info->create_info;
  DBUG_ENTER("ha_foster::check_if_supported_inplace_alter");

  if (ha_alter_info->handler_flags & Alter_inplace_info::CHANGE_CREATE_OPTION)
  {
    ha_table_option_struct *param_new= info->option_struct;
    ha_table_option_struct *param_old= table->s->option_struct;

  }

  DBUG_RETURN(HA_ALTER_INPLACE_EXCLUSIVE_LOCK);
}


struct st_mysql_storage_engine foster_storage_engine=
        { MYSQL_HANDLERTON_INTERFACE_VERSION };

//#define MYSQL_SYSVAR_STR(name, varname, opt, comment, check, update, def) \/
static MYSQL_SYSVAR_STR(
        db,                       // name
        fstr_db_var,                   // varname
        PLUGIN_VAR_READONLY,            // opt
        "Location of the DB file", // comment
        NULL,                           // check
        NULL,                           // update
        "/home/stefan/mariadb/zero_log/db");
static MYSQL_SYSVAR_STR(
        logdir,                       // name
        fstr_logdir_var,                // varname
        PLUGIN_VAR_READONLY,            // opt
        "Location of the log directory", // comment
        NULL,                           // check
        NULL,                           // update
        "/home/stefan/mariadb/zero_log/log");

static struct st_mysql_sys_var* foster_system_variables[]= {
        MYSQL_SYSVAR(db),
        MYSQL_SYSVAR(logdir),
        NULL
};


mysql_declare_plugin(foster)
                {
                        MYSQL_STORAGE_ENGINE_PLUGIN,
                        &foster_storage_engine,
                        "foster",
                        "Stefan Hemmer, TU KL",
                        "foster storage engine",
                        PLUGIN_LICENSE_GPL,
                        foster_init_func,                            /* Plugin Init */
                        foster_deinit_func,                                         /* Plugin Deinit */
                        0x0001 /* 0.1 */,
                        NULL,                                  /* status variables */
                        foster_system_variables,                     /* system variables */
                        NULL,                                         /* config options */
                        0,                                            /* flags */
                }
        mysql_declare_plugin_end;
maria_declare_plugin(foster)
                {
                        MYSQL_STORAGE_ENGINE_PLUGIN,
                        &foster_storage_engine,
                        "foster",
                        "Stefan Hemmer, TU KL",
                        "foster storage engine",
                        PLUGIN_LICENSE_GPL,
                        foster_init_func,                            /* Plugin Init */
                        foster_deinit_func,                                         /* Plugin Deinit */
                        0x0001,                                       /* version number (0.1) */
                        NULL,                                  /* status variables */
                        foster_system_variables,                     /* system variables */
                        "0.1",                                        /* string version */
                        MariaDB_PLUGIN_MATURITY_EXPERIMENTAL          /* maturity */
                }
        maria_declare_plugin_end;
