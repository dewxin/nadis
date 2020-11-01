#include "server.h"

struct sharedObjectsStruct shared;
nadisServer server;

struct redisCommand redisCommandTable[] = {
    {"get",getCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"set",setCommand,-3,"wm",0,NULL,1,1,1,0,0},
    {"subscribe",subscribeCommand,-2,"pslt",0,NULL,0,0,0,0,0},
    {"unsubscribe",unsubscribeCommand,-1,"pslt",0,NULL,0,0,0,0,0},
    {"publish",publishCommand,3,"pltF",0,NULL,0,0,0,0,0},
    {"expire",expireCommand,3,"wF",0,NULL,1,1,1,0,0},
    //todo.1 unsubscribe
   
};

/* Return the UNIX time in microseconds */
long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

long long mstime(void) {
    return ustime()/1000;
}

uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

uint64_t dictObjHash(const void *key) {
    const robj *o = key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

int dictObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    const robj *o1 = key1, *o2 = key2;
    return dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
}

void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

void dictObjectDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Lazy freeing will set value to NULL. */
    decrRefCount(val);
}

void dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

uint64_t dictSdsCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

//ignore the case 
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcasecmp(key1, key2) == 0;
}

uint64_t dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (sdsEncodedObject(o)) {
        return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
    } else {
        if (o->encoding == OBJ_ENCODING_INT) {
            char buf[32];
            int len;

            len = ll2string(buf,32,(long)o->ptr);
            return dictGenHashFunction((unsigned char*)buf, len);
        } else {
            uint64_t hash;

            o = getDecodedObject(o);
            hash = dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
            decrRefCount(o);
            return hash;
        }
    }
}

int dictEncObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    robj *o1 = (robj*) key1, *o2 = (robj*) key2;
    int cmp;

    if (o1->encoding == OBJ_ENCODING_INT &&
        o2->encoding == OBJ_ENCODING_INT)
            return o1->ptr == o2->ptr;

    o1 = getDecodedObject(o1);
    o2 = getDecodedObject(o2);
    cmp = dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
    decrRefCount(o1);
    decrRefCount(o2);
    return cmp;
}

/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType dbDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictObjectDestructor   /* val destructor */
};

/* Db->expires */
dictType keyptrDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL                        /* val destructor */
};

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL                        /* val destructor */
};

dictType keylistDictType = {
    dictObjHash,                // key hash function 
    NULL,                       // key dup
    NULL,                       // val dup
    dictObjKeyCompare,          // key compare
    dictObjectDestructor,       // key destructor
    dictListDestructor,         // val destructor
};

dictType objectKeyPointerValueDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictObjectDestructor, /* key destructor */
    NULL                       /* val destructor */
};

struct redisCommand *lookupCommand(sds name) {
    return dictFetchValue(server.commands, name);
}


void serverLogRaw(int level, const char *msg) {
    const int syslogLevelMap[] = { LOG_DEBUG, LOG_INFO, LOG_NOTICE, LOG_WARNING };
    const char *c = ".-*#";
    FILE *fp;
    char buf[64];
    int rawmode = (level & LL_RAW);
    int log_to_stdout = 1;
    //int log_to_stdout = server.logfile[0] == '\0';

    level &= 0xff; /* clear flags */
    if (level < server.verbosity) return;

    fp = stdout; //log_to_stdout ? stdout : fopen(server.logfile,"a");
    if (!fp) return;

    if (rawmode) {
        fprintf(fp,"%s",msg);
    } else {
        int off;
        struct timeval tv;
        int role_char;
        pid_t pid = getpid();

        gettimeofday(&tv,NULL);
        off = strftime(buf,sizeof(buf),"%d %b %H:%M:%S.",localtime(&tv.tv_sec));
        snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);
        // if (server.sentinel_mode) {
        //     role_char = 'X'; /* Sentinel. */
        // } else if (pid != server.pid) {
        //     role_char = 'C'; /* RDB / AOF writing child. */
        // } else {
        //     role_char = (server.masterhost ? 'S':'M'); /* Slave or Master. */
        // }
        fprintf(fp,"%d:%c %s %c %s\n",
            (int)getpid(),role_char, buf,c[level],msg);
    }
    fflush(fp);

    if (!log_to_stdout) fclose(fp);
    //if (server.syslog_enabled) syslog(syslogLevelMap[level], "%s", msg);
}

void serverPanic(const char *str) {
    serverLog(LL_WARNING, str);
}

void serverLog(int level, const char *fmt, ...) {
    va_list ap;
    char msg[LOG_MAX_LEN];

    if ((level&0xff) < server.verbosity) return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    serverLogRaw(level,msg);
}

/* Populates the Redis Command Table starting from the hard coded list
 * we have on top of redis.c file. */
void populateCommandTable(void) {
    int j;
    int numcommands = sizeof(redisCommandTable)/sizeof(struct redisCommand);

    for (j = 0; j < numcommands; j++) {
        struct redisCommand *c = redisCommandTable+j;
        char *f = c->sflags;
        int retval1;//, retval2;

        while(*f != '\0') {
            switch(*f) {
            case 'w': c->flags |= CMD_WRITE; break;
            case 'r': c->flags |= CMD_READONLY; break;
            case 'm': c->flags |= CMD_DENYOOM; break;
            case 'a': c->flags |= CMD_ADMIN; break;
            case 'p': c->flags |= CMD_PUBSUB; break;
            case 's': c->flags |= CMD_NOSCRIPT; break;
            case 'R': c->flags |= CMD_RANDOM; break;
            case 'S': c->flags |= CMD_SORT_FOR_SCRIPT; break;
            case 'l': c->flags |= CMD_LOADING; break;
            case 't': c->flags |= CMD_STALE; break;
            case 'M': c->flags |= CMD_SKIP_MONITOR; break;
            case 'k': c->flags |= CMD_ASKING; break;
            case 'F': c->flags |= CMD_FAST; break;
            default: serverPanic("Unsupported command flag"); break;
            }
            f++;
        }

        retval1 = dictAdd(server.commands, sdsnew(c->name), c);
        /* Populate an additional dictionary that will be unaffected
         * by rename-command statements in redis.conf. */
        // retval2 = dictAdd(server.orig_commands, sdsnew(c->name), c); todo
        serverAssert(retval1 == DICT_OK );//&& retval2 == DICT_OK);
    }
}


void initServerConfig(void) {

    server.port = CONFIG_DEFAULT_SERVER_PORT;
    server.bindaddr_count = 0;
    server.notify_keyspace_events = 0;
    server.dbnum = CONFIG_DEFAULT_DBNUM;
    server.hz = CONFIG_DEFAULT_HZ;
    server.active_expire_enabled = 1;
    server.ipfd_count = 0;
    server.maxclients = CONFIG_DEFAULT_MAX_CLIENTS;
    server.proto_max_bulk_len = CONFIG_DEFAULT_PROTO_MAX_BULK_LEN ; /* Bulk request max size */
    server.next_client_id = 1;
    server.commands = dictCreate(&commandTableDictType, NULL);
    
    populateCommandTable();
}

/**
 * @brief 
 *  listen the port use different address and 
 *  store the socket file descriptor in the fds array
 * @param port listen port
 * @param fds socket descriptor array
 * @param count  opened socket file descriptor count
 * @return int whether succeed
 */
int listenToPort(int port, int *fds, int *count) {
    int j;

    if(server.bindaddr_count == 0) server.bindaddr[0] = NULL;

    for (j = 0; j < server.bindaddr_count || j == 0; j++) {
        if (server.bindaddr[j] == NULL) {
            int unsupported = 0;
            /* Bind * for both IPv6 and IPv4, we enter here only if
             * server.bindaddr_count == 0. */
            fds[*count] = anetTcp6Server(server.neterr,port,NULL,
                server.tcp_backlog);
            if (fds[*count] != ANET_ERR) {
                anetNonBlock(NULL,fds[*count]);
                (*count)++;
            } else if (errno == EAFNOSUPPORT) {
                unsupported++;
                serverLog(LL_WARNING,"Not listening to IPv6: unsupproted");
            }

            if (*count == 1 || unsupported) {
                /* Bind the IPv4 address as well. */
                fds[*count] = anetTcpServer(server.neterr,port,NULL,
                    server.tcp_backlog);
                if (fds[*count] != ANET_ERR) {
                    anetNonBlock(NULL,fds[*count]);
                    (*count)++;
                } else if (errno == EAFNOSUPPORT) {
                    unsupported++;
                    serverLog(LL_WARNING,"Not listening to IPv4: unsupproted");
                }
            }
            /* Exit the loop if we were able to bind * on IPv4 and IPv6,
             * otherwise fds[*count] will be ANET_ERR and we'll print an
             * error and return to the caller with an error. */
            if (*count + unsupported == 2) break;
        } else if (strchr(server.bindaddr[j],':')) {
            /* Bind IPv6 address. */
            fds[*count] = anetTcp6Server(server.neterr,port,server.bindaddr[j],
                server.tcp_backlog);
        } else {
            /* Bind IPv4 address. */
            fds[*count] = anetTcpServer(server.neterr,port,server.bindaddr[j],
                server.tcp_backlog);
        }

        if (fds[*count] == ANET_ERR) {
            serverLog(LL_WARNING,
                "Creating Server TCP listening socket %s:%d: %s",
                server.bindaddr[j] ? server.bindaddr[j] : "*",
                port, server.neterr);
                if (errno == ENOPROTOOPT     || errno == EPROTONOSUPPORT ||
                    errno == ESOCKTNOSUPPORT || errno == EPFNOSUPPORT ||
                    errno == EAFNOSUPPORT    || errno == EADDRNOTAVAIL)
                    continue;
            return C_ERR;
        }
        anetNonBlock(NULL,fds[*count]);
        (*count)++;
    }

    return C_OK;

}

void createSharedObjects(void) {
    int j;

    shared.crlf = createObject(OBJ_STRING,sdsnew("\r\n"));
    shared.ok = createObject(OBJ_STRING,sdsnew("+OK\r\n"));
    shared.err = createObject(OBJ_STRING,sdsnew("-ERR\r\n"));
    shared.emptybulk = createObject(OBJ_STRING,sdsnew("$0\r\n\r\n"));
    shared.czero = createObject(OBJ_STRING,sdsnew(":0\r\n"));
    shared.cone = createObject(OBJ_STRING,sdsnew(":1\r\n"));
    shared.cnegone = createObject(OBJ_STRING,sdsnew(":-1\r\n"));
    shared.nullbulk = createObject(OBJ_STRING,sdsnew("$-1\r\n"));
    shared.nullmultibulk = createObject(OBJ_STRING,sdsnew("*-1\r\n"));
    shared.emptymultibulk = createObject(OBJ_STRING,sdsnew("*0\r\n"));
    shared.pong = createObject(OBJ_STRING,sdsnew("+PONG\r\n"));
    shared.queued = createObject(OBJ_STRING,sdsnew("+QUEUED\r\n"));
    shared.emptyscan = createObject(OBJ_STRING,sdsnew("*2\r\n$1\r\n0\r\n*0\r\n"));
    shared.wrongtypeerr = createObject(OBJ_STRING,sdsnew(
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"));
    shared.nokeyerr = createObject(OBJ_STRING,sdsnew(
        "-ERR no such key\r\n"));
    shared.syntaxerr = createObject(OBJ_STRING,sdsnew(
        "-ERR syntax error\r\n"));
    shared.sameobjecterr = createObject(OBJ_STRING,sdsnew(
        "-ERR source and destination objects are the same\r\n"));
    shared.outofrangeerr = createObject(OBJ_STRING,sdsnew(
        "-ERR index out of range\r\n"));
    shared.noscripterr = createObject(OBJ_STRING,sdsnew(
        "-NOSCRIPT No matching script. Please use EVAL.\r\n"));
    shared.loadingerr = createObject(OBJ_STRING,sdsnew(
        "-LOADING Redis is loading the dataset in memory\r\n"));
    shared.slowscripterr = createObject(OBJ_STRING,sdsnew(
        "-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"));
    shared.masterdownerr = createObject(OBJ_STRING,sdsnew(
        "-MASTERDOWN Link with MASTER is down and slave-serve-stale-data is set to 'no'.\r\n"));
    shared.bgsaveerr = createObject(OBJ_STRING,sdsnew(
        "-MISCONF Redis is configured to save RDB snapshots, but it is currently not able to persist on disk. Commands that may modify the data set are disabled, because this instance is configured to report errors during writes if RDB snapshotting fails (stop-writes-on-bgsave-error option). Please check the Redis logs for details about the RDB error.\r\n"));
    shared.roslaveerr = createObject(OBJ_STRING,sdsnew(
        "-READONLY You can't write against a read only slave.\r\n"));
    shared.noautherr = createObject(OBJ_STRING,sdsnew(
        "-NOAUTH Authentication required.\r\n"));
    shared.oomerr = createObject(OBJ_STRING,sdsnew(
        "-OOM command not allowed when used memory > 'maxmemory'.\r\n"));
    shared.execaborterr = createObject(OBJ_STRING,sdsnew(
        "-EXECABORT Transaction discarded because of previous errors.\r\n"));
    shared.noreplicaserr = createObject(OBJ_STRING,sdsnew(
        "-NOREPLICAS Not enough good slaves to write.\r\n"));
    shared.busykeyerr = createObject(OBJ_STRING,sdsnew(
        "-BUSYKEY Target key name already exists.\r\n"));
    shared.space = createObject(OBJ_STRING,sdsnew(" "));
    shared.colon = createObject(OBJ_STRING,sdsnew(":"));
    shared.plus = createObject(OBJ_STRING,sdsnew("+"));

    for (j = 0; j < PROTO_SHARED_SELECT_CMDS; j++) {
        char dictid_str[64];
        int dictid_len;

        dictid_len = ll2string(dictid_str,sizeof(dictid_str),j);
        shared.select[j] = createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, dictid_str));
    }
    shared.messagebulk = createStringObject("$7\r\nmessage\r\n",13);
    shared.pmessagebulk = createStringObject("$8\r\npmessage\r\n",14);
    shared.subscribebulk = createStringObject("$9\r\nsubscribe\r\n",15);
    shared.unsubscribebulk = createStringObject("$11\r\nunsubscribe\r\n",18);
    shared.psubscribebulk = createStringObject("$10\r\npsubscribe\r\n",17);
    shared.punsubscribebulk = createStringObject("$12\r\npunsubscribe\r\n",19);
    shared.del = createStringObject("DEL",3);
    shared.unlink = createStringObject("UNLINK",6);
    shared.rpop = createStringObject("RPOP",4);
    shared.lpop = createStringObject("LPOP",4);
    shared.lpush = createStringObject("LPUSH",5);
    shared.rpoplpush = createStringObject("RPOPLPUSH",9);
    // for (j = 0; j < OBJ_SHARED_INTEGERS; j++) { todo
    //     shared.integers[j] =
    //         makeObjectShared(createObject(OBJ_STRING,(void*)(long)j));
    //     shared.integers[j]->encoding = OBJ_ENCODING_INT;
    // }
    for (j = 0; j < OBJ_SHARED_BULKHDR_LEN; j++) {
        shared.mbulkhdr[j] = createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),"*%d\r\n",j));
        shared.bulkhdr[j] = createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),"$%d\r\n",j));
    }
    /* The following two shared objects, minstring and maxstrings, are not
     * actually used for their value but as a special object meaning
     * respectively the minimum possible string and the maximum possible
     * string in string comparisons for the ZRANGEBYLEX command. */
    shared.minstring = sdsnew("minstring");
    shared.maxstring = sdsnew("maxstring");
}

void resetServerStatus(void) {
    server.stat_expired_time_cap_reached_count = 0;
    server.stat_expired_stale_perc = 0;
}

void initServer(void) {
    int j;
    server.pid = getpid();


    server.clients = listCreate();
    server.clients_to_close = listCreate();
    server.clients_pending_write = listCreate();

    server.cronloops = 0;
    server.dirty = 0;
    resetServerStatus();

    createSharedObjects();
    server.el = aeCreateEventLoop(server.maxclients+128);
    if (server.el == NULL) {
        serverLog(LL_WARNING,
            "Failed creating the event loop. Error message: '%s'",
            strerror(errno));
        exit(1);
    }

    /* listen the port and store the ip file desciprtor and count */
    if(server.port !=0 && listenToPort(server.port, server.ipfd, &server.ipfd_count)== C_ERR)
        exit(1);

    /* Open the listening Unix domain socket. */

    if (server.ipfd_count == 0 /*&& server.sofd < 0 */) {
        serverLog(LL_WARNING, "Configured to not listen anywhere, exiting.");
        exit(1);
    }

    server.db = zmalloc(sizeof(redisDb)*server.dbnum);
    for(j = 0; j < server.dbnum; j++) {
        server.db[j].dict = dictCreate(&dbDictType,NULL);
        server.db[j].expires = dictCreate(&keyptrDictType,NULL);
        // server.db[j].blocking_keys = dictCreate(&keylistDictType,NULL);
        // server.db[j].ready_keys = dictCreate(&objectKeyPointerValueDictType,NULL);
        // server.db[j].watched_keys = dictCreate(&keylistDictType,NULL);
        server.db[j].id = j;
        server.db[j].avg_ttl = 0;
    }

    /* Create the timer callback, this is our way to process many background
     * operations incrementally, like clients timeout, eviction of unaccessed
     * expired keys and so forth. */
    if (aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL) == AE_ERR) {
        serverPanic("Can't create event loop timers.");
        exit(1);
    }

    server.pubsub_channels = dictCreate(&keylistDictType,NULL);

    for (j = 0; j < server.ipfd_count; j++) {
        if (aeCreateFileEvent(server.el, server.ipfd[j], AE_READABLE, acceptTcpHandler,NULL) == AE_ERR){
            serverLog(LL_WARNING, "Unrecoverable error creating server.ipfd file event.");
        }
    }

}

void call(client *c, int flags) {
    long long dirty, start, duration;
    int client_old_flags = c->flags;

    /* Sent the command to clients in MONITOR mode, only if the commands are
     * not generated from reading an AOF. */
    // if (listLength(server.monitors) &&
    //     !server.loading &&
    //     !(c->cmd->flags & (CMD_SKIP_MONITOR|CMD_ADMIN)))
    // {
    //     replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);
    // }

    /* Initialization: clear the flags that must be set by the command on
     * demand, and initialize the array for additional commands propagation. */
    // c->flags &= ~(CLIENT_FORCE_AOF|CLIENT_FORCE_REPL|CLIENT_PREVENT_PROP);
    // redisOpArray prev_also_propagate = server.also_propagate;
    // redisOpArrayInit(&server.also_propagate);

    /* Call the command. */
    dirty = server.dirty;
    start = ustime();
    c->cmd->proc(c);
    duration = ustime()-start;
    dirty = server.dirty-dirty;
    if (dirty < 0) dirty = 0;

    /* When EVAL is called loading the AOF we don't want commands called
     * from Lua to go into the slowlog or to populate statistics. */
    // if (server.loading && c->flags & CLIENT_LUA)
    //     flags &= ~(CMD_CALL_SLOWLOG | CMD_CALL_STATS);

    /* If the caller is Lua, we want to force the EVAL caller to propagate
     * the script if the command flag or client flag are forcing the
     * propagation. */
    // if (c->flags & CLIENT_LUA && server.lua_caller) {
    //     if (c->flags & CLIENT_FORCE_REPL)
    //         server.lua_caller->flags |= CLIENT_FORCE_REPL;
    //     if (c->flags & CLIENT_FORCE_AOF)
    //         server.lua_caller->flags |= CLIENT_FORCE_AOF;
    // }

    /* Log the command into the Slow log if needed, and populate the
     * per-command statistics that we show in INFO commandstats. */
    // if (flags & CMD_CALL_SLOWLOG && c->cmd->proc != execCommand) {
    //     char *latency_event = (c->cmd->flags & CMD_FAST) ?
    //                           "fast-command" : "command";
    //     latencyAddSampleIfNeeded(latency_event,duration/1000);
    //     slowlogPushEntryIfNeeded(c,c->argv,c->argc,duration);
    // }
    if (flags & CMD_CALL_STATS) {
        c->lastcmd->microseconds += duration;
        c->lastcmd->calls++;
    }

    /* Propagate the command into the AOF and replication link */
    // if (flags & CMD_CALL_PROPAGATE &&
    //     (c->flags & CLIENT_PREVENT_PROP) != CLIENT_PREVENT_PROP)
    // {
    //     int propagate_flags = PROPAGATE_NONE;

    //     /* Check if the command operated changes in the data set. If so
    //      * set for replication / AOF propagation. */
    //     if (dirty) propagate_flags |= (PROPAGATE_AOF|PROPAGATE_REPL);

    //     /* If the client forced AOF / replication of the command, set
    //      * the flags regardless of the command effects on the data set. */
    //     if (c->flags & CLIENT_FORCE_REPL) propagate_flags |= PROPAGATE_REPL;
    //     if (c->flags & CLIENT_FORCE_AOF) propagate_flags |= PROPAGATE_AOF;

    //     /* However prevent AOF / replication propagation if the command
    //      * implementatino called preventCommandPropagation() or similar,
    //      * or if we don't have the call() flags to do so. */
    //     if (c->flags & CLIENT_PREVENT_REPL_PROP ||
    //         !(flags & CMD_CALL_PROPAGATE_REPL))
    //             propagate_flags &= ~PROPAGATE_REPL;
    //     if (c->flags & CLIENT_PREVENT_AOF_PROP ||
    //         !(flags & CMD_CALL_PROPAGATE_AOF))
    //             propagate_flags &= ~PROPAGATE_AOF;

    //     /* Call propagate() only if at least one of AOF / replication
    //      * propagation is needed. Note that modules commands handle replication
    //      * in an explicit way, so we never replicate them automatically. */
    //     if (propagate_flags != PROPAGATE_NONE && !(c->cmd->flags & CMD_MODULE))
    //         propagate(c->cmd,c->db->id,c->argv,c->argc,propagate_flags);
    // }

    /* Restore the old replication flags, since call() can be executed
     * recursively. */
    // c->flags &= ~(CLIENT_FORCE_AOF|CLIENT_FORCE_REPL|CLIENT_PREVENT_PROP);
    // c->flags |= client_old_flags &
    //     (CLIENT_FORCE_AOF|CLIENT_FORCE_REPL|CLIENT_PREVENT_PROP);

    /* Handle the alsoPropagate() API to handle commands that want to propagate
     * multiple separated commands. Note that alsoPropagate() is not affected
     * by CLIENT_PREVENT_PROP flag. */
    // if (server.also_propagate.numops) {
    //     int j;
    //     redisOp *rop;

    //     if (flags & CMD_CALL_PROPAGATE) {
    //         for (j = 0; j < server.also_propagate.numops; j++) {
    //             rop = &server.also_propagate.ops[j];
    //             int target = rop->target;
    //             /* Whatever the command wish is, we honor the call() flags. */
    //             if (!(flags&CMD_CALL_PROPAGATE_AOF)) target &= ~PROPAGATE_AOF;
    //             if (!(flags&CMD_CALL_PROPAGATE_REPL)) target &= ~PROPAGATE_REPL;
    //             if (target)
    //                 propagate(rop->cmd,rop->dbid,rop->argv,rop->argc,target);
    //         }
    //     }
    //     redisOpArrayFree(&server.also_propagate);
    // }
    // server.also_propagate = prev_also_propagate;
    // server.stat_numcommands++; todo.1
}

int processCommand(client *c) {
    /* The QUIT command is handled separately. Normal command procs will
     * go through checking for replication and QUIT will cause trouble
     * when FORCE_REPLICATION is enabled and would be implemented in
     * a regular command proc. */
    if (!strcasecmp(c->argv[0]->ptr,"quit")) {  
        addReply(c,shared.ok);
        c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        return C_ERR;
    }
    // addReply(c,shared.ok);
    // return C_OK; // only allow quit command here

    /* Now lookup the command and check ASAP about trivial error conditions
     * such as wrong arity, bad command name and so forth. */
    c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr); 
    if (!c->cmd) {
        // flagTransaction(c);
        // sds args = sdsempty();
        // int i;
        // for (i=0; i < c->argc && sdslen(args) < 128; i++)
        //     args = sdscatprintf(args, "`%.*s`, ", 127-(int)sdslen(args), (char*)c->argv[i]->ptr);
        // addReplyErrorFormat(c,"unknown command `%s`, with args beginning with: %s",
        //     (char*)c->argv[-1]->ptr, args);
        // sdsfree(args);
        addReply(c,shared.ok);
        return C_OK;
    } else
    
    if ((c->cmd->arity > -1 && c->cmd->arity != c->argc) ||
               (c->argc < -c->cmd->arity)) {
        //flagTransaction(c); todo
        // addReplyErrorFormat(c,"wrong number of arguments for '%s' command", todo.1
        //     c->cmd->name); todo
       
        addReply(c,shared.ok);
        return C_OK;
    }

    /* Check if the user is authenticated */
    // if (server.requirepass && !c->authenticated && c->cmd->proc != authCommand)
    // {
    //     flagTransaction(c);
    //     addReply(c,shared.noautherr);
    //     return C_OK;
    // }

    /* If cluster is enabled perform the cluster redirection here.
     * However we don't perform the redirection if:
     * 0) The sender of this command is our master.
     * 1) The command has no key arguments. */
    // if (server.cluster_enabled &&
    //     !(c->flags & CLIENT_MASTER) &&
    //     !(c->flags & CLIENT_LUA &&
    //       server.lua_caller->flags & CLIENT_MASTER) &&
    //     !(c->cmd->getkeys_proc == NULL && c->cmd->firstkey == -1 &&
    //       c->cmd->proc != execCommand))
    // {
    //     int hashslot;
    //     int error_code;
    //     clusterNode *n = getNodeByQuery(c,c->cmd,c->argv,c->argc,
    //                                     &hashslot,&error_code);
    //     if (n == NULL || n != server.cluster->myself) {
    //         if (c->cmd->proc == execCommand) {
    //             discardTransaction(c);
    //         } else {
    //             flagTransaction(c);
    //         }
    //         clusterRedirectClient(c,n,hashslot,error_code);
    //         return C_OK;
    //     }
    // }

    /* Handle the maxmemory directive.
     *
     * First we try to free some memory if possible (if there are volatile
     * keys in the dataset). If there are not the only thing we can do
     * is returning an error. */
    // if (server.maxmemory) { //todo 
    //     int retval = freeMemoryIfNeeded();
    //     /* freeMemoryIfNeeded may flush slave output buffers. This may result
    //      * into a slave, that may be the active client, to be freed. */
    //     if (server.current_client == NULL) return C_ERR;

    //     /* It was impossible to free enough memory, and the command the client
    //      * is trying to execute is denied during OOM conditions? Error. */
    //     if ((c->cmd->flags & CMD_DENYOOM) && retval == C_ERR) {
    //         flagTransaction(c);
    //         addReply(c, shared.oomerr);
    //         return C_OK;
    //     }
    // }

    /* Don't accept write commands if there are problems persisting on disk
     * and if this is a master instance. */
    // if (((server.stop_writes_on_bgsave_err && todo
    //       server.saveparamslen > -1 &&
    //       server.lastbgsave_status == C_ERR) ||
    //       server.aof_last_write_status == C_ERR) &&
    //     server.masterhost == NULL &&
    //     (c->cmd->flags & CMD_WRITE ||
    //      c->cmd->proc == pingCommand))
    // {
    //     flagTransaction(c);
    //     if (server.aof_last_write_status == C_OK)
    //         addReply(c, shared.bgsaveerr);
    //     else
    //         addReplySds(c,
    //             sdscatprintf(sdsempty(),
    //             "-MISCONF Errors writing to the AOF file: %s\r\n",
    //             strerror(server.aof_last_write_errno)));
    //     return C_OK;
    // }

    /* Don't accept write commands if there are not enough good slaves and
     * user configured the min-slaves-to-write option. */
    // if (server.masterhost == NULL &&
    //     server.repl_min_slaves_to_write &&
    //     server.repl_min_slaves_max_lag &&
    //     c->cmd->flags & CMD_WRITE &&
    //     server.repl_good_slaves_count < server.repl_min_slaves_to_write)
    // {
    //     flagTransaction(c);
    //     addReply(c, shared.noreplicaserr);
    //     return C_OK;
    // }

    /* Don't accept write commands if this is a read only slave. But
     * accept write commands if this is our master. */
    // if (server.masterhost && server.repl_slave_ro &&
    //     !(c->flags & CLIENT_MASTER) &&
    //     c->cmd->flags & CMD_WRITE)
    // {
    //     addReply(c, shared.roslaveerr);
    //     return C_OK;
    // }

    /* Only allow SUBSCRIBE and UNSUBSCRIBE in the context of Pub/Sub */
    if (c->flags & CLIENT_PUBSUB &&
        //c->cmd->proc != pingCommand &&
        c->cmd->proc != subscribeCommand &&
        c->cmd->proc != unsubscribeCommand ) {
        //c->cmd->proc != psubscribeCommand &&
        //c->cmd->proc != punsubscribeCommand) {
        //addReplyError(c,"only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context"); todo
        return C_OK;
    }

    /* Only allow INFO and SLAVEOF when slave-serve-stale-data is no and
     * we are a slave with a broken link with master. */
    // if (server.masterhost && server.repl_state != REPL_STATE_CONNECTED &&
    //     server.repl_serve_stale_data == -1 &&
    //     !(c->cmd->flags & CMD_STALE))
    // {
    //     flagTransaction(c);
    //     addReply(c, shared.masterdownerr);
    //     return C_OK;
    // }

    /* Loading DB? Return an error if the command has not the
     * CMD_LOADING flag. */
    // if (server.loading && !(c->cmd->flags & CMD_LOADING)) {
    //     addReply(c, shared.loadingerr);
    //     return C_OK;
    // }

    /* Lua script too slow? Only allow a limited number of commands. */
    // if (server.lua_timedout &&
    //       c->cmd->proc != authCommand &&
    //       c->cmd->proc != replconfCommand &&
    //     !(c->cmd->proc == shutdownCommand &&
    //       c->argc == 1 &&
    //       tolower(((char*)c->argv[0]->ptr)[0]) == 'n') &&
    //     !(c->cmd->proc == scriptCommand &&
    //       c->argc == 1 &&
    //       tolower(((char*)c->argv[0]->ptr)[0]) == 'k'))
    // {
    //     flagTransaction(c);
    //     addReply(c, shared.slowscripterr);
    //     return C_OK;
    // }

    
    /* Exec the command */
    // if (c->flags & CLIENT_MULTI &&
    //     c->cmd->proc != execCommand && c->cmd->proc != discardCommand &&
    //     c->cmd->proc != multiCommand && c->cmd->proc != watchCommand)
    // {
    //     queueMultiCommand(c);
    //     addReply(c,shared.queued);
    // } else {
        call(c,CMD_CALL_FULL);
        //c->woff = server.master_repl_offset;
        // if (listLength(server.ready_keys))
        //     handleClientsBlockedOnLists(); todo
    // }
    return C_OK;

}




/* This function handles 'background' operations we are required to do
 * incrementally in Redis databases, such as active key expiring, resizing,
 * rehashing. */
void databasesCron(void) {
    /* Expire keys by random sampling. Not required for slaves
     * as master will synthesize DELs for us. */
    if (server.active_expire_enabled /*&& server.masterhost == NULL*/) {
        activeExpireCycle(ACTIVE_EXPIRE_CYCLE_SLOW);
    // } else if (server.masterhost != NULL) {
    //     expireSlaveKeys();
    }

    /* Defrag keys gradually. */
    // if (server.active_defrag_enabled)
    //     activeDefragCycle();

    /* Perform hash tables rehashing if needed, but only if there are no
     * other processes saving the DB on disk. Otherwise rehashing is bad
     * as will cause a lot of copy-on-write of memory pages. */
    // if (server.rdb_child_pid == -1 && server.aof_child_pid == -1) {
    //     /* We use global counters so if we stop the computation at a given
    //      * DB we'll be able to start from the successive in the next
    //      * cron loop iteration. */
    //     static unsigned int resize_db = 0;
    //     static unsigned int rehash_db = 0;
    //     int dbs_per_call = CRON_DBS_PER_CALL;
    //     int j;

    //     /* Don't test more DBs than we have. */
    //     if (dbs_per_call > server.dbnum) dbs_per_call = server.dbnum;

    //     /* Resize */
    //     for (j = 0; j < dbs_per_call; j++) {
    //         tryResizeHashTables(resize_db % server.dbnum);
    //         resize_db++;
    //     }

    //     /* Rehash */
    //     if (server.activerehashing) {
    //         for (j = 0; j < dbs_per_call; j++) {
    //             int work_done = incrementallyRehash(rehash_db);
    //             if (work_done) {
    //                 /* If the function did some work, stop here, we'll do
    //                  * more at the next cron loop. */
    //                 break;
    //             } else {
    //                 /* If this db didn't need rehash, we'll try the next one. */
    //                 rehash_db++;
    //                 rehash_db %= server.dbnum;
    //             }
    //         }
    //     }
    // }
}


/* This is our timer interrupt, called server.hz times per second.
 * Here is where we do a number of things that need to be done asynchronously.
 * For instance:
 *
 * - Active expired keys collection (it is also performed in a lazy way on
 *   lookup).
 * - Software watchdog.
 * - Update some statistic.
 * - Incremental rehashing of the DBs hash tables.
 * - Triggering BGSAVE / AOF rewrite, and handling of terminated children.
 * - Clients timeout of different kinds.
 * - Replication reconnection.
 * - Many more...
 *
 * Everything directly called here will be called server.hz times per second,
 * so in order to throttle execution of things we want to do less frequently
 * a macro is used: run_with_period(milliseconds) { .... }
 */

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    int j;
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    /* Software watchdog: deliver the SIGALRM that will reach the signal
     * handler if we don't return here fast enough. */
    // if (server.watchdog_period) watchdogScheduleSignal(server.watchdog_period);

    /* Update the time cache. */
    //updateCachedTime();

    // run_with_period(100) {
    //     trackInstantaneousMetric(STATS_METRIC_COMMAND,server.stat_numcommands);
    //     trackInstantaneousMetric(STATS_METRIC_NET_INPUT,
    //             server.stat_net_input_bytes);
    //     trackInstantaneousMetric(STATS_METRIC_NET_OUTPUT,
    //             server.stat_net_output_bytes);
    // }

    /* We have just LRU_BITS bits per object for LRU information.
     * So we use an (eventually wrapping) LRU clock.
     *
     * Note that even if the counter wraps it's not a big problem,
     * everything will still work but some object will appear younger
     * to Redis. However for this to happen a given object should never be
     * touched for all the time needed to the counter to wrap, which is
     * not likely.
     *
     * Note that you can change the resolution altering the
     * LRU_CLOCK_RESOLUTION define. */
    // unsigned long lruclock = getLRUClock();
    // atomicSet(server.lruclock,lruclock);

    /* Record the max memory used since the server was started. */
    // if (zmalloc_used_memory() > server.stat_peak_memory)
    //     server.stat_peak_memory = zmalloc_used_memory();

    /* Sample the RSS here since this is a relatively slow call. */
    // server.resident_set_size = zmalloc_get_rss();

    /* We received a SIGTERM, shutting down here in a safe way, as it is
     * not ok doing so inside the signal handler. */
    // if (server.shutdown_asap) {
    //     if (prepareForShutdown(SHUTDOWN_NOFLAGS) == C_OK) exit(0);
    //     serverLog(LL_WARNING,"SIGTERM received but errors trying to shut down the server, check the logs for more information");
    //     server.shutdown_asap = 0;
    // }

    /* Show some info about non-empty databases */
    // run_with_period(5000) {
    //     for (j = 0; j < server.dbnum; j++) {
    //         long long size, used, vkeys;

    //         size = dictSlots(server.db[j].dict);
    //         used = dictSize(server.db[j].dict);
    //         vkeys = dictSize(server.db[j].expires);
    //         if (used || vkeys) {
    //             serverLog(LL_VERBOSE,"DB %d: %lld keys (%lld volatile) in %lld slots HT.",j,used,vkeys,size);
    //             /* dictPrintStats(server.dict); */
    //         }
    //     }
    // }

    /* Show information about connected clients */
    // if (!server.sentinel_mode) {
    //     run_with_period(5000) {
    //         serverLog(LL_VERBOSE,
    //             "%lu clients connected (%lu slaves), %zu bytes in use",
    //             listLength(server.clients)-listLength(server.slaves),
    //             listLength(server.slaves),
    //             zmalloc_used_memory());
    //     }
    // }

    /* We need to do a few operations on clients asynchronously. */
    //clientsCron();

    /* Handle background operations on Redis databases. */
    databasesCron();

    /* Start a scheduled AOF rewrite if this was requested by the user while
     * a BGSAVE was in progress. */
    // if (server.rdb_child_pid == -1 && server.aof_child_pid == -1 &&
    //     server.aof_rewrite_scheduled)
    // {
    //     rewriteAppendOnlyFileBackground();
    // }

    /* Check if a background saving or AOF rewrite in progress terminated. */
    // if (server.rdb_child_pid != -1 || server.aof_child_pid != -1 ||
    //     ldbPendingChildren())
    // {
    //     int statloc;
    //     pid_t pid;

    //     if ((pid = wait3(&statloc,WNOHANG,NULL)) != 0) {
    //         int exitcode = WEXITSTATUS(statloc);
    //         int bysignal = 0;

    //         if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);

    //         if (pid == -1) {
    //             serverLog(LL_WARNING,"wait3() returned an error: %s. "
    //                 "rdb_child_pid = %d, aof_child_pid = %d",
    //                 strerror(errno),
    //                 (int) server.rdb_child_pid,
    //                 (int) server.aof_child_pid);
    //         } else if (pid == server.rdb_child_pid) {
    //             backgroundSaveDoneHandler(exitcode,bysignal);
    //             if (!bysignal && exitcode == 0) receiveChildInfo();
    //         } else if (pid == server.aof_child_pid) {
    //             backgroundRewriteDoneHandler(exitcode,bysignal);
    //             if (!bysignal && exitcode == 0) receiveChildInfo();
    //         } else {
    //             if (!ldbRemoveChild(pid)) {
    //                 serverLog(LL_WARNING,
    //                     "Warning, detected child with unmatched pid: %ld",
    //                     (long)pid);
    //             }
    //         }
    //         updateDictResizePolicy();
    //         closeChildInfoPipe();
    //     }
    // } else {
    //     /* If there is not a background saving/rewrite in progress check if
    //      * we have to save/rewrite now. */
    //      for (j = 0; j < server.saveparamslen; j++) {
    //         struct saveparam *sp = server.saveparams+j;

    //         /* Save if we reached the given amount of changes,
    //          * the given amount of seconds, and if the latest bgsave was
    //          * successful or if, in case of an error, at least
    //          * CONFIG_BGSAVE_RETRY_DELAY seconds already elapsed. */
    //         if (server.dirty >= sp->changes &&
    //             server.unixtime-server.lastsave > sp->seconds &&
    //             (server.unixtime-server.lastbgsave_try >
    //              CONFIG_BGSAVE_RETRY_DELAY ||
    //              server.lastbgsave_status == C_OK))
    //         {
    //             serverLog(LL_NOTICE,"%d changes in %d seconds. Saving...",
    //                 sp->changes, (int)sp->seconds);
    //             rdbSaveInfo rsi, *rsiptr;
    //             rsiptr = rdbPopulateSaveInfo(&rsi);
    //             rdbSaveBackground(server.rdb_filename,rsiptr);
    //             break;
    //         }
    //      }

    //      /* Trigger an AOF rewrite if needed. */
    //      if (server.aof_state == AOF_ON &&
    //          server.rdb_child_pid == -1 &&
    //          server.aof_child_pid == -1 &&
    //          server.aof_rewrite_perc &&
    //          server.aof_current_size > server.aof_rewrite_min_size)
    //      {
    //         long long base = server.aof_rewrite_base_size ?
    //                         server.aof_rewrite_base_size : 1;
    //         long long growth = (server.aof_current_size*100/base) - 100;
    //         if (growth >= server.aof_rewrite_perc) {
    //             serverLog(LL_NOTICE,"Starting automatic rewriting of AOF on %lld%% growth",growth);
    //             rewriteAppendOnlyFileBackground();
    //         }
    //      }
    // }


    /* AOF postponed flush: Try at every cron cycle if the slow fsync
     * completed. */
    // if (server.aof_flush_postponed_start) flushAppendOnlyFile(0);

    /* AOF write errors: in this case we have a buffer to flush as well and
     * clear the AOF error in case of success to make the DB writable again,
     * however to try every second is enough in case of 'hz' is set to
     * an higher frequency. */
    // run_with_period(1000) {
    //     if (server.aof_last_write_status == C_ERR)
    //         flushAppendOnlyFile(0);
    // }

    /* Close clients that need to be closed asynchronous */
    // freeClientsInAsyncFreeQueue();

    /* Clear the paused clients flag if needed. */
    // clientsArePaused(); /* Don't check return value, just use the side effect.*/

    /* Replication cron function -- used to reconnect to master,
     * detect transfer failures, start background RDB transfers and so forth. */
    // run_with_period(1000) replicationCron();

    /* Run the Redis Cluster cron. */
    // run_with_period(100) {
    //     if (server.cluster_enabled) clusterCron();
    // }

    /* Run the Sentinel timer if we are in sentinel mode. */
    // run_with_period(100) {
    //     if (server.sentinel_mode) sentinelTimer();
    // }

    /* Cleanup expired MIGRATE cached sockets. */
    // run_with_period(1000) {
    //     migrateCloseTimedoutSockets();
    // }

    /* Start a scheduled BGSAVE if the corresponding flag is set. This is
     * useful when we are forced to postpone a BGSAVE because an AOF
     * rewrite is in progress.
     *
     * Note: this code must be after the replicationCron() call above so
     * make sure when refactoring this file to keep this order. This is useful
     * because we want to give priority to RDB savings for replication. */
    // if (server.rdb_child_pid == -1 && server.aof_child_pid == -1 &&
    //     server.rdb_bgsave_scheduled &&
    //     (server.unixtime-server.lastbgsave_try > CONFIG_BGSAVE_RETRY_DELAY ||
    //      server.lastbgsave_status == C_OK))
    // {
    //     rdbSaveInfo rsi, *rsiptr;
    //     rsiptr = rdbPopulateSaveInfo(&rsi);
    //     if (rdbSaveBackground(server.rdb_filename,rsiptr) == C_OK)
    //         server.rdb_bgsave_scheduled = 0;
    // }

    server.cronloops++;
    return 1000/server.hz;
}

/* This function gets called every time Redis is entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors. */
void beforeSleep(struct aeEventLoop *eventLoop) {
    // UNUSED(eventLoop);

    /* Call the Redis Cluster before sleep function. Note that this function
     * may change the state of Redis Cluster (from ok to fail or vice versa),
     * so it's a good idea to call it before serving the unblocked clients
     * later in this function. */
    // if (server.cluster_enabled) clusterBeforeSleep();

    /* Run a fast expire cycle (the called function will return
     * ASAP if a fast cycle is not needed). */
    // if (server.active_expire_enabled && server.masterhost == NULL)
    //     activeExpireCycle(ACTIVE_EXPIRE_CYCLE_FAST);

    /* Send all the slaves an ACK request if at least one client blocked
     * during the previous event loop iteration. */
    // if (server.get_ack_from_slaves) {
    //     robj *argv[3];

    //     argv[0] = createStringObject("REPLCONF",8);
    //     argv[1] = createStringObject("GETACK",6);
    //     argv[2] = createStringObject("*",1); /* Not used argument. */
    //     replicationFeedSlaves(server.slaves, server.slaveseldb, argv, 3);
    //     decrRefCount(argv[0]);
    //     decrRefCount(argv[1]);
    //     decrRefCount(argv[2]);
    //     server.get_ack_from_slaves = 0;
    // }

    /* Unblock all the clients blocked for synchronous replication
     * in WAIT. */
    // if (listLength(server.clients_waiting_acks))
    //     processClientsWaitingReplicas();

    // /* Check if there are clients unblocked by modules that implement
    //  * blocking commands. */
    // moduleHandleBlockedClients();

    // /* Try to process pending commands for clients that were just unblocked. */
    // if (listLength(server.unblocked_clients))
    //     processUnblockedClients();

    // /* Write the AOF buffer on disk */
    // flushAppendOnlyFile(0);

    /* Handle writes with pending output buffers. */
    handleClientsWithPendingWrites();

    /* Before we are going to sleep, let the threads access the dataset by
     * releasing the GIL. Redis main thread will not touch anything at this
     * time. */
    // if (moduleCount()) moduleReleaseGIL();
}


int main(int argc, char *argv) {
    initServerConfig();
    serverLog(LL_WARNING, "oO0OoO0OoO0Oo Nadis is starting oO0OoO0OoO0Oo");

    initServer();

    aeSetBeforeSleepProc(server.el, beforeSleep);
    aeMain(server.el);
    aeDeleteEventLoop(server.el);
}
