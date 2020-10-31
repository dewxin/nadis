#include "server.h"

robj *createObject(int type, void *ptr) {
    robj *o = zmalloc(sizeof(*o));
    o->type = type;
    o->encoding = OBJ_ENCODING_RAW;
    o->ptr = ptr;
    o->refcount = 1;

    // /* Set the LRU to the current lruclock (minutes resolution), or
    //  * alternatively the LFU counter. */
    // if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
    //     o->lru = (LFUGetTimeInMinutes()<<8) | LFU_INIT_VAL;
    // } else {
    //     o->lru = LRU_CLOCK(); todo
    // }
    return o;
}

robj *createEmbeddedStringObject(const char *ptr, size_t len) {
    robj *o = zmalloc(sizeof(robj)+sizeof(struct sdshdr8)+len+1);
    struct sdshdr8 *sh = (void*)(o+1);

    o->type = OBJ_STRING;
    o->encoding = OBJ_ENCODING_EMBSTR;
    o->ptr = sh+1;
    o->refcount = 1;
    // if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
    //     o->lru = (LFUGetTimeInMinutes()<<8) | LFU_INIT_VAL;
    // } else {
    //     o->lru = LRU_CLOCK();
    // }

    sh->len = len;
    sh->alloc = len;
    sh->flags = SDS_TYPE_8;
    if (ptr) {
        memcpy(sh->buf,ptr,len);
        sh->buf[len] = '\0';
    } else {
        memset(sh->buf,0,len+1);
    }
    return o;
}

robj *createRawStringObject(const char *ptr, size_t len) {
    return createObject(OBJ_STRING, sdsnewlen(ptr,len));
}

#define OBJ_ENCODING_EMBSTR_SIZE_LIMIT 44
robj *createStringObject(const char *ptr, size_t len) {
    if (len <= OBJ_ENCODING_EMBSTR_SIZE_LIMIT)
        return createEmbeddedStringObject(ptr,len);
    else
        return createRawStringObject(ptr,len);
}

void freeStringObject(robj *o) {
    if (o->encoding == OBJ_ENCODING_RAW) {
        sdsfree(o->ptr);
    }
}

void freeListObject(robj *o) {
    // if (o->encoding == OBJ_ENCODING_QUICKLIST) { todo
    //     quicklistRelease(o->ptr);
    // } else {
    //     serverPanic("Unknown list encoding type");
    // }
}

void freeSetObject(robj *o) {
    // switch (o->encoding) { todo
    // case OBJ_ENCODING_HT:
    //     dictRelease((dict*) o->ptr);
    //     break;
    // case OBJ_ENCODING_INTSET:
    //     zfree(o->ptr);
    //     break;
    // default:
    //     serverPanic("Unknown set encoding type");
    // }
}

void freeZsetObject(robj *o) {
    // zset *zs;
    // switch (o->encoding) { todo
    // case OBJ_ENCODING_SKIPLIST:
    //     zs = o->ptr;
    //     dictRelease(zs->dict);
    //     zslFree(zs->zsl);
    //     zfree(zs);
    //     break;
    // case OBJ_ENCODING_ZIPLIST:
    //     zfree(o->ptr);
    //     break;
    // default:
    //     serverPanic("Unknown sorted set encoding");
    // }
}

void freeHashObject(robj *o) {
    // switch (o->encoding) { todo
    // case OBJ_ENCODING_HT:
    //     dictRelease((dict*) o->ptr);
    //     break;
    // case OBJ_ENCODING_ZIPLIST:
    //     zfree(o->ptr);
    //     break;
    // default:
    //     serverPanic("Unknown hash encoding type");
    //     break;
    // }
}



void decrRefCount(robj *o) {
    if (o->refcount == 1) {
        switch(o->type) {
        case OBJ_STRING: freeStringObject(o); break;
        case OBJ_LIST: freeListObject(o); break;
        case OBJ_SET: freeSetObject(o); break;
        case OBJ_HASH: freeHashObject(o); break;
        case OBJ_ZSET: freeZsetObject(o); break;
        //case OBJ_MODULE: freeModuleObject(o); break;
        default: serverPanic("Unknown object type"); break;
        }
        zfree(o);
    } else {
        if (o->refcount <= 0) serverPanic("decrRefCount against refcount <= 0");
        if (o->refcount != OBJ_SHARED_REFCOUNT) o->refcount--;
    }
}

int getLongLongFromObject(robj *o, long long *target) {
    long long value;

    if (o == NULL) {
        value = 0;
    } else {
        //serverAssertWithInfo(NULL,o,o->type == OBJ_STRING); todo
        if (sdsEncodedObject(o)) {
            if (string2ll(o->ptr,sdslen(o->ptr),&value) == 0) return C_ERR;
        } else if (o->encoding == OBJ_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            serverPanic("Unknown string encoding");
        }
    }
    if (target) *target = value;
    return C_OK;
}

int getLongLongFromObjectOrReply(client *c, robj *o, long long *target, const char *msg) {
    long long value;
    if (getLongLongFromObject(o, &value) != C_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not an integer or out of range");
        }
        return C_ERR;
    }
    *target = value;
    return C_OK;
}


void incrRefCount(robj *o) {
    if (o->refcount != OBJ_SHARED_REFCOUNT) o->refcount++;
}

robj *getDecodedObject(robj *o) {
    robj *dec;

    if (sdsEncodedObject(o)) {
        incrRefCount(o);
        return o;
    }
    if (o->type == OBJ_STRING && o->encoding == OBJ_ENCODING_INT) {
        char buf[32];

        ll2string(buf,32,(long)o->ptr);
        dec = createStringObject(buf,strlen(buf));
        return dec;
    } else {
        serverPanic("Unknown encoding type");
    }
}