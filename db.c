#include "server.h"


int selectDb(client *c, int id) {
    if (id < 0 || id >= server.dbnum)
        return C_ERR;
    c->db = &server.db[id];
    return C_OK;
}


robj *lookupKey(redisDb *db, robj *key, int flags) {
    dictEntry *de = dictFind(db->dict,key->ptr);
    if (de) {
        robj *val = dictGetVal(de);

        /* Update the access time for the ageing algorithm.
         * Don't do it if we have a saving child, as this will trigger
         * a copy on write madness. */
        // if (server.rdb_child_pid == -1 && todo.1
        //     server.aof_child_pid == -1 &&
        //     !(flags & LOOKUP_NOTOUCH))
        // {
        //     if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        //         updateLFU(val);
        //     } else {
        //         val->lru = LRU_CLOCK();
        //     }
        // }
        return val;
    } else {
        return NULL;
    }
}

/* Lookup a key for read operations, or return NULL if the key is not found
 * in the specified DB.
 *
 * As a side effect of calling this function:
 * 1. A key gets expired if it reached it's TTL.
 * 2. The key last access time is updated.
 * 3. The global keys hits/misses stats are updated (reported in INFO).
 *
 * This API should not be used when we write to the key after obtaining
 * the object linked to the key, but only for read only operations.
 *
 * Flags change the behavior of this command:
 *
 *  LOOKUP_NONE (or zero): no special flags are passed.
 *  LOOKUP_NOTOUCH: don't alter the last access time of the key.
 *
 * Note: this function also returns NULL is the key is logically expired
 * but still existing, in case this is a slave, since this API is called only
 * for read operations. Even if the key expiry is master-driven, we can
 * correctly report a key is expired on slaves even if the master is lagging
 * expiring our key via DELs in the replication link. */
robj *lookupKeyReadWithFlags(redisDb *db, robj *key, int flags) {
    robj *val;

    // if (expireIfNeeded(db,key) == 1) { todo
    //     /* Key expired. If we are in the context of a master, expireIfNeeded()
    //      * returns 0 only when the key does not exist at all, so it's safe
    //      * to return NULL ASAP. */
    //     if (server.masterhost == NULL) return NULL;

    //     /* However if we are in the context of a slave, expireIfNeeded() will
    //      * not really try to expire the key, it only returns information
    //      * about the "logical" status of the key: key expiring is up to the
    //      * master in order to have a consistent view of master's data set.
    //      *
    //      * However, if the command caller is not the master, and as additional
    //      * safety measure, the command invoked is a read-only command, we can
    //      * safely return NULL here, and provide a more consistent behavior
    //      * to clients accessign expired values in a read-only fashion, that
    //      * will say the key as non exisitng.
    //      *
    //      * Notably this covers GETs when slaves are used to scale reads. */
    //     if (server.current_client &&
    //         server.current_client != server.master &&
    //         server.current_client->cmd &&
    //         server.current_client->cmd->flags & CMD_READONLY)
    //     {
    //         return NULL;
    //     }
    // }
    val = lookupKey(db,key,flags);
    // if (val == NULL)
    //     server.stat_keyspace_misses++;
    // else
    //     server.stat_keyspace_hits++;
    return val;
}

robj *lookupKeyRead(redisDb *db, robj *key) {
    return lookupKeyReadWithFlags(db,key,LOOKUP_NONE);
}


robj *lookupKeyReadOrReply(client *c, robj *key, robj *reply) {
    robj *o = lookupKeyRead(c->db, key);
    if (!o) addReply(c,reply);
    return o;
}


robj *lookupKeyWrite(redisDb *db, robj *key) {
    expireIfNeeded(db,key); 
    return lookupKey(db,key,LOOKUP_NONE);
}

/* Add the key to the DB. It's up to the caller to increment the reference
 * counter of the value if needed.
 *
 * The program is aborted if the key already exists. */
void dbAdd(redisDb *db, robj *key, robj *val) {
    sds copy = sdsdup(key->ptr);
    int retval = dictAdd(db->dict, copy, val);

    //serverAssertWithInfo(NULL,key,retval == DICT_OK); todo
    //if (val->type == OBJ_LIST) signalListAsReady(db, key);
    //if (server.cluster_enabled) slotToKeyAdd(key);
 }

/* Delete a key, value, and associated expiration entry if any, from the DB */
int dbSyncDelete(redisDb *db, robj *key) {
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */
    if (dictSize(db->expires) > 0) dictDelete(db->expires, key->ptr);
    if (dictDelete(db->dict, key->ptr) == DICT_OK) {
        //if (server.cluster_enabled) slotToKeyDel(key);
        return 1;
    } else {
        return 0;
    }
}

/* Return the expire time of the specified key, or -1 if no expire
 * is associated with this key (i.e. the key is non volatile) */
long long getExpire(redisDb *db, robj *key) {
    dictEntry *de;

    /* No expire? return ASAP */
    if (dictSize(db->expires) == 0 ||
       (de = dictFind(db->expires,key->ptr)) == NULL) return -1;

    /* The entry was found in the expire dict, this means it should also
     * be present in the main dict (safety check). */
    serverAssertWithInfo(NULL,key,dictFind(db->dict,key->ptr) != NULL);
    return dictGetSignedIntegerVal(de);
}
/* This function is called when we are going to perform some operation
 * in a given key, but such key may be already logically expired even if
 * it still exists in the database. The main way this function is called
 * is via lookupKey*() family of functions.
 *
 * The behavior of the function depends on the replication role of the
 * instance, because slave instances do not expire keys, they wait
 * for DELs from the master for consistency matters. However even
 * slaves will try to have a coherent return value for the function,
 * so that read commands executed in the slave side will be able to
 * behave like if the key is expired even if still present (because the
 * master has yet to propagate the DEL).
 *
 * In masters as a side effect of finding a key which is expired, such
 * key will be evicted from the database. Also this may trigger the
 * propagation of a DEL/UNLINK command in AOF / replication stream.
 *
 * The return value of the function is 0 if the key is still valid,
 * otherwise the function returns 1 if the key is expired. */
int expireIfNeeded(redisDb *db, robj *key) {
    mstime_t when = getExpire(db,key);
    mstime_t now;

    if (when < 0) return 0; /* No expire for this key */

    /* Don't expire anything while loading. It will be done later. */
    //if (server.loading) return 0; todo.2

    /* If we are in the context of a Lua script, we pretend that time is
     * blocked to when the Lua script started. This way a key can expire
     * only the first time it is accessed and not in the middle of the
     * script execution, making propagation to slaves / AOF consistent.
     * See issue #1525 on Github for more information. */
    now = //server.lua_caller ? server.lua_time_start :
             mstime();

    /* If we are running in the context of a slave, return ASAP:
     * the slave key expiration is controlled by the master that will
     * send us synthesized DEL operations for expired keys.
     *
     * Still we try to return the right information to the caller,
     * that is, 0 if we think the key should be still valid, 1 if
     * we think the key is expired at this time. */
    //if (server.masterhost != NULL) return now > when;

    /* Return when this key has not expired */
    if (now <= when) return 0;

    /* Delete the key */
    //server.stat_expiredkeys++; todo.1
    //propagateExpire(db,key,server.lazyfree_lazy_expire); todo.2
    notifyKeyspaceEvent(NOTIFY_EXPIRED,
        "expired",key,db->id);
    return //server.lazyfree_lazy_expire ? dbAsyncDelete(db,key) :
                                         dbSyncDelete(db,key);
}

void setExpire(client *c, redisDb *db, robj *key, long long when) {
    dictEntry *kde, *de;

    /* Reuse the sds from the main dict in the expire dict */
    kde = dictFind(db->dict,key->ptr);
    serverAssertWithInfo(NULL,key,kde != NULL);
    de = dictAddOrFind(db->expires,dictGetKey(kde));
    dictSetSignedIntegerVal(de,when);

    // int writable_slave = server.masterhost && server.repl_slave_ro == 0;
    // if (c && writable_slave && !(c->flags & CLIENT_MASTER))
    //     rememberSlaveKeyWithExpire(db,key);
}

int removeExpire(redisDb *db, robj *key) {
    /* An expire may only be removed if there is a corresponding entry in the
     * main dict. Otherwise, the key will never be freed. */
    serverAssertWithInfo(NULL,key,dictFind(db->dict,key->ptr) != NULL);
    return dictDelete(db->expires,key->ptr) == DICT_OK;
}

void setKey(redisDb *db, robj *key, robj *val) {
    if (lookupKeyWrite(db,key) == NULL) {
        dbAdd(db,key,val);
    // } else {
    //     dbOverwrite(db,key,val); todo.1
    }
    incrRefCount(val);
    removeExpire(db,key);
    //signalModifiedKey(db,key); todo.1
}

