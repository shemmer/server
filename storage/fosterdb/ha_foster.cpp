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

#include "ha_foster.h"
#include <key.h>


static FOSTER_SHARE *get_share(const char *table_name);
static int free_share(FOSTER_SHARE *share);
static HASH foster_open_tables;
mysql_mutex_t foster_mutex;

static handler *foster_create_handler(handlerton *hton,
                                      TABLE_SHARE *table,
                                      MEM_ROOT *mem_root);


fstr_wrk_thr_t* ha_foster::main_thread;


#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key key_mutex_foster_share, key_mutex_foster;

static PSI_mutex_key key_mutex_foster_wrk;

static PSI_mutex_info all_foster_mutexes[]=
        {
                { &key_mutex_foster_share, "foster_share::mutex", 0},
                { &key_mutex_foster, "foster", PSI_FLAG_GLOBAL}
        };

static PSI_cond_key key_COND_work;


static PSI_cond_info all_fstr_cond[]=
        {
                { &key_COND_work, "COND_work", 0},
        };


static void init_foster_psi_keys()
{
  const char* category= "foster";
  int count;

  count= array_elements(all_foster_mutexes);
  mysql_mutex_register(category, all_foster_mutexes, count);
  count= array_elements(all_fstr_cond);
  mysql_cond_register(category, all_fstr_cond, count);
}
#endif


static const char *ha_foster_exts[] = {
        NullS
};

static uchar* foster_get_key(FOSTER_SHARE *share, size_t *length,
                           my_bool not_used __attribute__((unused)))
{
  *length=share->table_name_length;
  return (uchar*) share->table_name;
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


    start_stop_request_t* req = new start_stop_request_t();
    //Locking work request mutex
    mysql_mutex_init(key_mutex_foster_wrk, &req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
    mysql_mutex_lock(&req->LOCK_work_mutex);
    //Init the request condition -> used to signal the handler when the worker is done
    mysql_cond_init(key_COND_work, &req->COND_work, NULL);
    req->type=FOSTER_STARTUP;

    ha_foster::main_thread = new fstr_wrk_thr_t(false);
    mysql_mutex_lock(&ha_foster::main_thread->thread_mutex);
    ha_foster::main_thread->set_request(req);
    ha_foster::main_thread->notify(true);
    mysql_cond_signal(&ha_foster::main_thread->COND_worker);
    mysql_mutex_unlock(&ha_foster::main_thread->thread_mutex);

    while(!req->notified){
      mysql_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
    }

    mysql_mutex_unlock(&req->LOCK_work_mutex);
    mysql_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
  DBUG_RETURN(0);
}


static int foster_deinit_func(void *p){
  DBUG_ENTER("foster_deinit_func");
  start_stop_request_t* req = new start_stop_request_t();

  //Locking work request mutex
  mysql_mutex_init(key_mutex_foster_wrk, &req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
  mysql_mutex_lock(&req->LOCK_work_mutex);
  //Init the request condition -> used to signal the handler when the worker is done
  mysql_cond_init(key_COND_work, &req->COND_work, NULL);

  req->type=FOSTER_SHUTDOWN;

//    mysql_cond_signal(&ha_foster::main_thread->COND_worker);
  mysql_mutex_lock(&ha_foster::main_thread->thread_mutex);
  ha_foster::main_thread->set_request(req);
  ha_foster::main_thread->notify(true);
  mysql_cond_broadcast(&ha_foster::main_thread->COND_worker);
  mysql_mutex_unlock(&ha_foster::main_thread->thread_mutex);

  while(!req->notified){
    mysql_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }

  mysql_mutex_unlock(&req->LOCK_work_mutex);
  delete(req);

  ha_foster::main_thread->foster_exit();

  delete(ha_foster::main_thread);

  my_hash_free(&foster_open_tables);
  mysql_mutex_destroy(&foster_mutex);
  DBUG_RETURN(0);
}




static FOSTER_SHARE *get_share(const char *table_name)
{
  FOSTER_SHARE *share;
  char *tmp_name;
  uint length;
  mysql_mutex_lock(&foster_mutex);
  length=(uint) strlen(table_name);

  /*
    If share is not present in the hash, create a new share and
    initialize its members.
  */
  if (!(share=(FOSTER_SHARE*) my_hash_search(&foster_open_tables,
                                           (uchar*) table_name,
                                           length)))
  {
    if (!my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
                         &share, sizeof(*share),
                         &tmp_name, length+1,
                         NullS))
    {
      mysql_mutex_unlock(&foster_mutex);
      return NULL;
    }

    share->use_count= 0;
    share->table_name_length= length;
    share->table_name= tmp_name;

    strmov(share->table_name, table_name);

    if (my_hash_insert(&foster_open_tables, (uchar*) share))
      goto error;
    thr_lock_init(&share->lock);
    mysql_mutex_init(key_mutex_foster_share,
                     &share->mutex, MY_MUTEX_INIT_FAST);
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
    thr_lock_delete(&share->lock);
    mysql_mutex_destroy(&share->mutex);
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
  DBUG_RETURN(0);
}


int ha_foster::close(void)
{
  int rc= 0;
  DBUG_ENTER("ha_foster::close");
  destroy_record_buffer(record_buffer);
  my_free(key_buffer);
  DBUG_RETURN(free_share(share) || rc);
}


int ha_foster::create(const char *name, TABLE *table_arg,
                      HA_CREATE_INFO *create_info)
{
  int rc=0;
  DBUG_ENTER("ha_foster::create");

  ddl_request_t* req = new ddl_request_t();
  //Locking work request mutex
  mysql_mutex_init(key_mutex_foster_wrk, &req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
  mysql_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  mysql_cond_init(key_COND_work, &req->COND_work, NULL);
  req->type=FOSTER_CREATE;
  req->table_name.set(name, strlen(name), &my_charset_bin);
  req->table= table_arg;

  fstr_wrk_thr_t* func_worker = new fstr_wrk_thr_t(true);
  mysql_mutex_lock(&func_worker->thread_mutex);
  func_worker->set_request(req);
  func_worker->notify(true);
  mysql_cond_broadcast(&func_worker->COND_worker);
  mysql_mutex_unlock(&func_worker->thread_mutex);

  while(!req->notified){
    mysql_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }

  mysql_mutex_unlock(&req->LOCK_work_mutex);
  func_worker->foster_exit();
  delete(func_worker);

  mysql_mutex_destroy(&req->LOCK_work_mutex);
  delete(req);
  DBUG_RETURN(rc);
}



int ha_foster::delete_table(const char *name)
{
  int rc=0;
  DBUG_ENTER("ha_foster::delete_table");
  ddl_request_t* req = new ddl_request_t();
  mysql_mutex_init(key_mutex_foster_wrk, &req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
  mysql_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  mysql_cond_init(key_COND_work, &req->COND_work, NULL);

  fstr_wrk_thr_t* del_worker = new fstr_wrk_thr_t(true);

  req->type=FOSTER_DELETE;
  req->table_name.set(name, strlen(name), &my_charset_bin);
  req->table=table;

  mysql_mutex_lock(&del_worker->thread_mutex);
  del_worker->set_request(req);
  del_worker->notify(true);
  mysql_cond_broadcast(&del_worker ->COND_worker);
  mysql_mutex_unlock(&del_worker->thread_mutex);

  while(!req->notified){
    mysql_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  rc=req->err;
  mysql_mutex_unlock(&req->LOCK_work_mutex);
  mysql_mutex_destroy(&req->LOCK_work_mutex);
  delete(req);
  del_worker->foster_exit();
  delete(del_worker);
  DBUG_RETURN(rc);
}

int ha_foster::write_row(uchar *buf) {
  int rc=0;
  DBUG_ENTER("ha_foster::write_row");
  write_request_t *req = new write_request_t();

  //Locking work request mutex
  mysql_mutex_init(key_mutex_foster_wrk, &req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
  mysql_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  mysql_cond_init(key_COND_work, &req->COND_work, NULL);


  //Initializing values of the request
  req->type = FOSTER_WRITE_ROW;
  req->key_buf=key_buffer;
  req->table_name.set(table_name, strlen(table_name), &my_charset_bin);

  req->mysql_format_buf=buf;

  if (fix_rec_buff(max_row_length(buf)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  req->rec_buf=record_buffer->buffer;

  req->table = table;
  req->max_key_len=max_key_len;

  mysql_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  mysql_cond_broadcast(&worker->COND_worker);
  mysql_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    mysql_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  mysql_mutex_unlock(&req->LOCK_work_mutex);
  rc=req->err;
  mysql_mutex_destroy(&req->LOCK_work_mutex);
  delete(req);
  DBUG_RETURN(rc);
}


int ha_foster::update_row(const uchar *old_data, uchar *new_data)
{
  int rc=0;
  DBUG_ENTER("ha_foster::update_row");
  write_request_t *req = new write_request_t();

  //Locking work request mutex
  mysql_mutex_init(key_mutex_foster_wrk, &req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
  mysql_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  mysql_cond_init(key_COND_work, &req->COND_work, NULL);
  req->table_name.set(table_name, strlen(table_name), &my_charset_bin);

  req->type = FOSTER_UPDATE_ROW;
  req->key_buf=key_buffer;

  req->mysql_format_buf=new_data;
  req->old_mysql_format_buf= const_cast<uchar*>(old_data);
  req->rec_buf=record_buffer->buffer;
  req->table=table;


  mysql_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  mysql_cond_broadcast(&worker->COND_worker);
  mysql_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    mysql_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }

  rc=req->err;

  mysql_mutex_unlock(&req->LOCK_work_mutex);

  mysql_mutex_destroy(&req->LOCK_work_mutex);
  delete(req);
  DBUG_RETURN(rc);
}


int ha_foster::delete_row(const uchar *buf)
{
  int rc=0;
  DBUG_ENTER("ha_foster::delete_row");
  write_request_t *req = new write_request_t();

  //Locking work request mutex
  mysql_mutex_init(key_mutex_foster_wrk, &req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
  mysql_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  mysql_cond_init(key_COND_work, &req->COND_work, NULL);

  req->type = FOSTER_DELETE_ROW;
  req->table_name.set(table_name, strlen(table_name), &my_charset_bin);

  req->table=table;

  req->key_buf=key_buffer;
  req->mysql_format_buf=const_cast<uchar*>(buf);

  mysql_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  mysql_cond_broadcast(&worker->COND_worker);
  mysql_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    mysql_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }

  mysql_mutex_unlock(&req->LOCK_work_mutex);

  if(req->err==0){
    mysql_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status=0;
    rc= 0;
  }else{

    mysql_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    rc=HA_ERR_END_OF_FILE;
  }
  DBUG_RETURN(rc);
}
int ha_foster::index_init(uint idx, bool sorted){
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

  //Locking work request mutex
  mysql_mutex_init(key_mutex_foster_wrk, &req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
  mysql_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  mysql_cond_init(key_COND_work, &req->COND_work, NULL);

  req->type = FOSTER_IDX_READ;
  req->table_name.set(table_name, strlen(table_name), &my_charset_bin);
  req->key_buf = const_cast<uchar*>(key);
  req->ksz=table->s->key_info[index].key_length;
  req->find_flag=find_flag;
  req->mysql_format_buf=buf;
  req->table=table;

  req->idx_no=index;

  mysql_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  mysql_cond_broadcast(&worker->COND_worker);
  mysql_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    mysql_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  mysql_mutex_unlock(&req->LOCK_work_mutex);

  if(req->err==0){
    rc=0;
    mysql_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status=0;
  }else{
    mysql_mutex_destroy(&req->LOCK_work_mutex);
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

  //Locking work request mutex
  mysql_mutex_init(key_mutex_foster_wrk, &req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
  mysql_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  mysql_cond_init(key_COND_work, &req->COND_work, NULL);

  req->type = FOSTER_IDX_READ;
  req->table_name.set(table_name, strlen(table_name), &my_charset_bin);
  req->key_buf = const_cast<uchar*>(key);
  req->ksz=key_len;
  req->find_flag=find_flag;
  req->mysql_format_buf=buf;
  req->table=table;

  req->idx_no=active_index;

  mysql_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  mysql_cond_broadcast(&worker->COND_worker);
  mysql_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    mysql_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  mysql_mutex_unlock(&req->LOCK_work_mutex);
  rc=req->err;
  if(req->err==0){
    mysql_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status=0;
  }else{
    mysql_mutex_destroy(&req->LOCK_work_mutex);
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
  mysql_mutex_init(key_mutex_foster_wrk, &req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
  mysql_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  mysql_cond_init(key_COND_work, &req->COND_work, NULL);

  req->type = FOSTER_IDX_NEXT;
  req->table_name.set(table_name, strlen(table_name), &my_charset_bin);
  req->mysql_format_buf=buf;
  req->table=table;
  req->idx_no=active_index;

  mysql_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  mysql_cond_broadcast(&worker->COND_worker);
  mysql_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    mysql_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  mysql_mutex_unlock(&req->LOCK_work_mutex);
  rc=req->err;
  if(req->err==0){
//    rc= unpack_row(req->buf, req->len, buf);
    mysql_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status=0;
  }else{
    mysql_mutex_destroy(&req->LOCK_work_mutex);
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
  int rc=0;
  DBUG_ENTER("ha_foster::rnd_next");

  table->status = 0;
  read_request_t *req = new read_request_t();

  mysql_mutex_init(key_mutex_foster_wrk, &req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
  mysql_mutex_lock(&req->LOCK_work_mutex);

  mysql_cond_init(key_COND_work, &req->COND_work, NULL);

  req->table_name.set(table_name, strlen(table_name), &my_charset_bin);
  req->mysql_format_buf=buf;
  req->table=table;
  req->idx_no=0;

  if(first_row){
    first_row=false;
    req->key_buf =0;
    req->ksz=0;
    req->find_flag=HA_READ_KEY_OR_NEXT;
    req->type = FOSTER_IDX_READ;
  }else{
    req->type = FOSTER_IDX_NEXT;
  }

  mysql_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  mysql_cond_broadcast(&worker->COND_worker);
  mysql_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    mysql_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  mysql_mutex_unlock(&req->LOCK_work_mutex);
  rc=req->err;
  if(req->err==0){
    mysql_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status=0;
  }else{
    mysql_mutex_destroy(&req->LOCK_work_mutex);
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

  //Locking work request mutex
  mysql_mutex_init(key_mutex_foster_wrk, &req->LOCK_work_mutex, MY_MUTEX_INIT_FAST);
  mysql_mutex_lock(&req->LOCK_work_mutex);

  //Init the request condition -> used to signal the handler when the worker is done
  mysql_cond_init(key_COND_work, &req->COND_work, NULL);

  req->table_name.set(table_name, strlen(table_name), &my_charset_bin);
  req->mysql_format_buf=buf;
  req->table=table;
  req->key_buf=pos;
  req->ksz=ref_length;

  req->type=FOSTER_POS_READ;

  mysql_mutex_lock(&worker->thread_mutex);
  worker->set_request(req);
  worker->notify(true);
  mysql_cond_broadcast(&worker->COND_worker);
  mysql_mutex_unlock(&worker->thread_mutex);

  while(!req->notified){
    mysql_cond_wait(&req->COND_work, &req->LOCK_work_mutex);
  }
  mysql_mutex_unlock(&req->LOCK_work_mutex);
  rc=req->err;
  if(req->err==0){
    mysql_mutex_destroy(&req->LOCK_work_mutex);
    delete(req);
    table->status=0;
  }else{
    mysql_mutex_destroy(&req->LOCK_work_mutex);
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
  my_free(r);
  DBUG_VOID_RETURN;
}

/*
  Calculate max length needed for row. This includes
  the bytes required for the length in the header.
*/

uint32 ha_foster::max_row_length(const uchar *buf)
{
  uint32 length= (uint32)(table->s->reclength + table->s->fields*2);

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
    if (!(newptr = (uchar *) my_realloc((uchar *) record_buffer->buffer,
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
  foster_record_buffer *r;
  if (!(r= (foster_record_buffer*) my_malloc(sizeof(foster_record_buffer),
                                                   MYF(MY_WME))))
  {
    DBUG_RETURN(NULL);
  }
  r->length= (int)length;

  if (!(r->buffer= (uchar*) my_malloc(r->length,
                                      MYF(MY_WME))))
  {
    my_free(r);
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
    /*
      This foster shows how custom engine specific table and field
      options can be accessed from this function to be compared.
    */
    ha_table_option_struct *param_new= info->option_struct;
    ha_table_option_struct *param_old= table->s->option_struct;

  }

  DBUG_RETURN(HA_ALTER_INPLACE_EXCLUSIVE_LOCK);
}


struct st_mysql_storage_engine foster_storage_engine=
        { MYSQL_HANDLERTON_INTERFACE_VERSION };

static struct st_mysql_sys_var* foster_system_variables[]= {
        NULL
};



mysql_declare_plugin(foster)
                {
                        MYSQL_STORAGE_ENGINE_PLUGIN,
                        &foster_storage_engine,
                        "foster",
                        "Brian Aker, MySQL AB",
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
                        "Brian Aker, MySQL AB",
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
