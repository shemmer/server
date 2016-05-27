/*
  Copyright (c) 2004, 2010, Oracle and/or its affiliates

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

/** @file ha_foster.h

*/

#ifdef USE_PRAGMA_INTERFACE
#pragma interface     /* gcc class implementation */
#endif

#include <my_config.h>

#include "sql_table.h"

#include "sql_class.h"

#include "fstr_wrk_thr_t.h"

typedef struct st_foster_record_buffer {
    uchar *buffer;
    uint32 length;
} foster_record_buffer;


typedef struct st_foster_idx {
    char* index_name;
    uint index_name_length;
    uint stid;
} foster_index;

static uchar* foster_index_get_key(foster_index* idx, size_t *length,
                            my_bool not_used __attribute__((unused)))
{
    *length=idx->index_name_length;
    return (uchar*) idx->index_name;
}


class FOSTER_SHARE: Handler_share{
public:
    char *table_name;
    uint table_name_length, use_count;
    mysql_mutex_t mutex;
    THR_LOCK lock;

    HASH foster_indexes;

    ~FOSTER_SHARE(){
        thr_lock_delete(&lock);
        mysql_mutex_destroy(&mutex);
    }


    void init_hash(){
        (void) my_hash_init(&foster_indexes,system_charset_info,32,0,0,
                            (my_hash_get_key) foster_index_get_key,0,0);
    }

    foster_index* get_storeid(char* idx_name_buf, uint length){
        foster_index* fosteridx;
        char *tmp_name;
        if (!(fosteridx=(foster_index*) my_hash_search(&foster_indexes, (uchar*) idx_name_buf, length)))
        {
                return NULL;
        }
        return fosteridx;
    }

    bool add_storeid(char* idx_name_buf, uint length, uint storeid){
        char *tmp_name;
        foster_index* fosteridx;
        if (!my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
                         &fosteridx, sizeof(*fosteridx),
                         &tmp_name, length+1,
                         NullS))
        {
            return false;
        }

        fosteridx->index_name=tmp_name;
        fosteridx->index_name_length=length;
        fosteridx->stid=storeid;

        strmov(fosteridx->index_name, idx_name_buf);

        if (my_hash_insert(&foster_indexes, (uchar*) fosteridx)){
            return false;
        }
        return true;
    }

    std::vector<uint> get_all_storeids(){
        std::vector<uint> stid_vector(foster_indexes.array.elements);
        for(int i=0; i< foster_indexes.array.elements; i++){
            foster_index* index = (foster_index*) my_hash_element(&foster_indexes, i);
            if(strcmp(index->index_name,"PRIMARY")==0 && i!=0){
                uint tmp = stid_vector[0];
                stid_vector[0]=index->stid;
                stid_vector[i]=tmp;
            }else {
                stid_vector[i] = index->stid;
            }
        }
        return stid_vector;
    }
};

/** @brief
  Class definition for the storage engine
*/
class ha_foster: public handler
{
    THR_LOCK_DATA lock;      ///< MySQL lock
    FOSTER_SHARE *share;    ///< Shared lock info
    const char* table_name;
    uchar *key_buffer;

    fstr_wrk_thr_t* worker;

    bool first_row;
    uint active_index;

    foster_record_buffer* record_buffer;

    uint max_key_len = 0;

    uint max_key_name_len=0;
public:

    static fstr_wrk_thr_t* main_thread;
    ha_foster(handlerton *hton, TABLE_SHARE *table_arg);
    ~ha_foster()
    {
    }
    const char *index_type(uint inx) { return "BTREE"; }

    ulonglong table_flags() const
    {
        return HA_TABLE_SCAN_ON_INDEX |
               HA_NULL_IN_KEY |
               HA_CAN_SQL_HANDLER |
               HA_REQUIRES_KEY_COLUMNS_FOR_DELETE |
               HA_PRIMARY_KEY_REQUIRED_FOR_POSITION |
               HA_PRIMARY_KEY_REQUIRED_FOR_DELETE |
               HA_REQUIRE_PRIMARY_KEY |
               HA_PRIMARY_KEY_IN_READ_INDEX|
               HA_REC_NOT_IN_SEQ |
               HA_AUTO_PART_KEY |
               HA_CAN_INDEX_BLOBS |
               HA_NO_PREFIX_CHAR_KEYS|
               HA_NO_AUTO_INCREMENT|
               HA_ANY_INDEX_MAY_BE_UNIQUE|
               HA_HAS_OWN_BINLOGGING
                ;
    }

    ulong index_flags(uint inx, uint part, bool all_parts) const
    {
        return HA_READ_NEXT | HA_READ_ORDER | HA_READ_RANGE | HA_ONLY_WHOLE_INDEX;
    }

    uint max_supported_record_length() const { return HA_MAX_REC_LENGTH; }

    uint max_supported_keys()          const { return 10; }


    uint max_supported_key_parts()     const { return 10; }


    uint max_supported_key_length()    const { return 128; }


    virtual double scan_time() { return (double) (stats.records+stats.deleted) / 20.0+10; }

    virtual double read_time(uint, uint, ha_rows rows)
    { return (double) rows /  20.0+1; }


    int open(const char *name, int mode, uint test_if_locked);    // required

    int close(void);                                              // required

    int write_row(uchar *buf);

    int update_row(const uchar *old_data, uchar *new_data);

    int delete_row(const uchar *buf);

    int index_init(uint idx, bool sorted);
    int index_end();
    int index_read_idx_map(uchar * buf, uint index, const uchar * key,
                           key_part_map keypart_map,
                           enum ha_rkey_function find_flag);

    int index_read(uchar *buf, const uchar *key, uint key_len,
                   enum ha_rkey_function find_flag);
    int index_next(uchar *buf);

//  int index_prev(uchar *buf);

    int index_first(uchar *buf);

    int index_last(uchar *buf);

    int rnd_init(bool scan);
    int rnd_next(uchar *buf);                                     ///< required
    int rnd_pos(uchar *buf, uchar *pos);                          ///< required
    void position(const uchar *record);                           ///< required
    int info(uint);                                               ///< required
    int extra(enum ha_extra_function operation);
    int external_lock(THD *thd, int lock_type);                   ///< required
    int delete_all_rows(void);
    ha_rows records_in_range(uint inx, key_range *min_key,
                             key_range *max_key);
    int delete_table(const char *from);
    int create(const char *name, TABLE *form,
               HA_CREATE_INFO *create_info);                      ///< required

    bool fix_rec_buff(unsigned int length);


    void destroy_record_buffer(foster_record_buffer *r);
    foster_record_buffer* create_record_buffer(ulong length);

    uint32 max_row_length(const uchar *buf);
    enum_alter_inplace_result
            check_if_supported_inplace_alter(TABLE* altered_table,
                                             Alter_inplace_info* ha_alter_info);

    THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                               enum thr_lock_type lock_type);     ///< required
};
