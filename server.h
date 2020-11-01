#ifndef __SERVER_H
#define __SERVER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <syslog.h>
#include <netinet/in.h>
#include <stdarg.h>
#include <sys/time.h>
typedef long long mstime_t; /* millisecond time type. */

#include "dict.h"
#include "anet.h"
#include "ae.h"
#include "zmalloc.h"
#include "adlist.h"
#include "sds.h"
#include "util.h"

/* Anti-warning macro... */
#define UNUSED(V) ((void) V)

/* Error codes */
#define C_OK                    0
#define C_ERR                   -1

/* Static server configuration */
#define CONFIG_DEFAULT_HZ        10      /* Time interrupt calls/sec. */
#define CONFIG_MIN_HZ            1
#define CONFIG_MAX_HZ            500
#define CONFIG_DEFAULT_SERVER_PORT        6379    /* TCP port */
#define CONFIG_DEFAULT_TCP_BACKLOG       511     /* TCP listen backlog */
#define CONFIG_DEFAULT_CLIENT_TIMEOUT       0       /* default client timeout: infinite */
#define CONFIG_DEFAULT_DBNUM     16
#define CONFIG_MAX_LINE    1024
#define CRON_DBS_PER_CALL 16
#define NET_MAX_WRITES_PER_EVENT (1024*64)
#define PROTO_SHARED_SELECT_CMDS 10
#define OBJ_SHARED_INTEGERS 10000
#define OBJ_SHARED_BULKHDR_LEN 32
#define LOG_MAX_LEN    1024 /* Default maximum length of syslog messages */
#define AOF_REWRITE_PERC  100
#define AOF_REWRITE_MIN_SIZE (64*1024*1024)
#define AOF_REWRITE_ITEMS_PER_CMD 64
#define AOF_READ_DIFF_INTERVAL_BYTES (1024*10)
#define CONFIG_DEFAULT_SLOWLOG_LOG_SLOWER_THAN 10000
#define CONFIG_DEFAULT_SLOWLOG_MAX_LEN 128
#define CONFIG_DEFAULT_MAX_CLIENTS 10000
#define CONFIG_AUTHPASS_MAX_LEN 512
#define CONFIG_DEFAULT_SLAVE_PRIORITY 100
#define CONFIG_DEFAULT_REPL_TIMEOUT 60
#define CONFIG_DEFAULT_REPL_PING_SLAVE_PERIOD 10
#define CONFIG_RUN_ID_SIZE 40
#define RDB_EOF_MARK_SIZE 40
#define CONFIG_DEFAULT_REPL_BACKLOG_SIZE (1024*1024)    /* 1mb */
#define CONFIG_DEFAULT_REPL_BACKLOG_TIME_LIMIT (60*60)  /* 1 hour */
#define CONFIG_REPL_BACKLOG_MIN_SIZE (1024*16)          /* 16k */
#define CONFIG_BGSAVE_RETRY_DELAY 5 /* Wait a few secs before trying again. */
#define CONFIG_DEFAULT_PID_FILE "/var/run/redis.pid"
#define CONFIG_DEFAULT_SYSLOG_IDENT "redis"
#define CONFIG_DEFAULT_CLUSTER_CONFIG_FILE "nodes.conf"
#define CONFIG_DEFAULT_CLUSTER_ANNOUNCE_IP NULL         /* Auto detect. */
#define CONFIG_DEFAULT_CLUSTER_ANNOUNCE_PORT 0          /* Use server.port */
#define CONFIG_DEFAULT_CLUSTER_ANNOUNCE_BUS_PORT 0      /* Use +10000 offset. */
#define CONFIG_DEFAULT_DAEMONIZE 0
#define CONFIG_DEFAULT_UNIX_SOCKET_PERM 0
#define CONFIG_DEFAULT_TCP_KEEPALIVE 300
#define CONFIG_DEFAULT_PROTECTED_MODE 1
#define CONFIG_DEFAULT_LOGFILE ""
#define CONFIG_DEFAULT_SYSLOG_ENABLED 0
#define CONFIG_DEFAULT_STOP_WRITES_ON_BGSAVE_ERROR 1
#define CONFIG_DEFAULT_RDB_COMPRESSION 1
#define CONFIG_DEFAULT_RDB_CHECKSUM 1
#define CONFIG_DEFAULT_RDB_FILENAME "dump.rdb"
#define CONFIG_DEFAULT_REPL_DISKLESS_SYNC 0
#define CONFIG_DEFAULT_REPL_DISKLESS_SYNC_DELAY 5
#define CONFIG_DEFAULT_SLAVE_SERVE_STALE_DATA 1
#define CONFIG_DEFAULT_SLAVE_READ_ONLY 1
#define CONFIG_DEFAULT_SLAVE_ANNOUNCE_IP NULL
#define CONFIG_DEFAULT_SLAVE_ANNOUNCE_PORT 0
#define CONFIG_DEFAULT_REPL_DISABLE_TCP_NODELAY 0
#define CONFIG_DEFAULT_MAXMEMORY 0
#define CONFIG_DEFAULT_MAXMEMORY_SAMPLES 5
#define CONFIG_DEFAULT_LFU_LOG_FACTOR 10
#define CONFIG_DEFAULT_LFU_DECAY_TIME 1
#define CONFIG_DEFAULT_AOF_FILENAME "appendonly.aof"
#define CONFIG_DEFAULT_AOF_NO_FSYNC_ON_REWRITE 0
#define CONFIG_DEFAULT_AOF_LOAD_TRUNCATED 1
#define CONFIG_DEFAULT_AOF_USE_RDB_PREAMBLE 0
#define CONFIG_DEFAULT_ACTIVE_REHASHING 1
#define CONFIG_DEFAULT_AOF_REWRITE_INCREMENTAL_FSYNC 1
#define CONFIG_DEFAULT_MIN_SLAVES_TO_WRITE 0
#define CONFIG_DEFAULT_MIN_SLAVES_MAX_LAG 10
#define NET_IP_STR_LEN 46 /* INET6_ADDRSTRLEN is 46, but we need to be sure */
#define NET_PEER_ID_LEN (NET_IP_STR_LEN+32) /* Must be enough for ip:port */
#define CONFIG_BINDADDR_MAX 16
#define CONFIG_MIN_RESERVED_FDS 32
#define CONFIG_DEFAULT_LATENCY_MONITOR_THRESHOLD 0
#define CONFIG_DEFAULT_SLAVE_LAZY_FLUSH 0
#define CONFIG_DEFAULT_LAZYFREE_LAZY_EVICTION 0
#define CONFIG_DEFAULT_LAZYFREE_LAZY_EXPIRE 0
#define CONFIG_DEFAULT_LAZYFREE_LAZY_SERVER_DEL 0
#define CONFIG_DEFAULT_ALWAYS_SHOW_LOGO 0
#define CONFIG_DEFAULT_ACTIVE_DEFRAG 0
#define CONFIG_DEFAULT_DEFRAG_THRESHOLD_LOWER 10 /* don't defrag when fragmentation is below 10% */
#define CONFIG_DEFAULT_DEFRAG_THRESHOLD_UPPER 100 /* maximum defrag force at 100% fragmentation */
#define CONFIG_DEFAULT_DEFRAG_IGNORE_BYTES (100<<20) /* don't defrag if frag overhead is below 100mb */
#define CONFIG_DEFAULT_DEFRAG_CYCLE_MIN 25 /* 25% CPU min (at lower threshold) */
#define CONFIG_DEFAULT_DEFRAG_CYCLE_MAX 75 /* 75% CPU max (at upper threshold) */
#define CONFIG_DEFAULT_PROTO_MAX_BULK_LEN (512ll*1024*1024) /* Bulk request max size */

#define ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP 20 /* Loopkups per loop. */
#define ACTIVE_EXPIRE_CYCLE_FAST_DURATION 1000 /* Microseconds */
#define ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC 25 /* CPU max % for keys collection */
#define ACTIVE_EXPIRE_CYCLE_SLOW 0
#define ACTIVE_EXPIRE_CYCLE_FAST 1

/* Units */
#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1

#define PROTO_REQ_INLINE 1
#define PROTO_REQ_MULTIBULK 2
#define NET_MAX_WRITES_PER_EVENT (1024*64)
#define PROTO_SHARED_SELECT_CMDS 10
#define OBJ_SHARED_INTEGERS 10000
#define OBJ_SHARED_BULKHDR_LEN 32

/* Client flags */
#define CLIENT_SLAVE (1<<0)   /* This client is a slave server */
#define CLIENT_MASTER (1<<1)  /* This client is a master server */
#define CLIENT_MONITOR (1<<2) /* This client is a slave monitor, see MONITOR */
#define CLIENT_MULTI (1<<3)   /* This client is in a MULTI context */
#define CLIENT_BLOCKED (1<<4) /* The client is waiting in a blocking operation */
#define CLIENT_DIRTY_CAS (1<<5) /* Watched keys modified. EXEC will fail. */
#define CLIENT_CLOSE_AFTER_REPLY (1<<6) /* Close after writing entire reply. */
#define CLIENT_UNBLOCKED (1<<7) /* This client was unblocked and is stored in
                                  server.unblocked_clients */
#define CLIENT_LUA (1<<8) /* This is a non connected client used by Lua */
#define CLIENT_ASKING (1<<9)     /* Client issued the ASKING command */
#define CLIENT_CLOSE_ASAP (1<<10)/* Close this client ASAP */
#define CLIENT_UNIX_SOCKET (1<<11) /* Client connected via Unix domain socket */
#define CLIENT_DIRTY_EXEC (1<<12)  /* EXEC will fail for errors while queueing */
#define CLIENT_MASTER_FORCE_REPLY (1<<13)  /* Queue replies even if is master */
#define CLIENT_FORCE_AOF (1<<14)   /* Force AOF propagation of current cmd. */
#define CLIENT_FORCE_REPL (1<<15)  /* Force replication of current cmd. */
#define CLIENT_PRE_PSYNC (1<<16)   /* Instance don't understand PSYNC. */
#define CLIENT_READONLY (1<<17)    /* Cluster client is in read-only state. */
#define CLIENT_PUBSUB (1<<18)      /* Client is in Pub/Sub mode. */
#define CLIENT_PREVENT_AOF_PROP (1<<19)  /* Don't propagate to AOF. */
#define CLIENT_PREVENT_REPL_PROP (1<<20)  /* Don't propagate to slaves. */
#define CLIENT_PREVENT_PROP (CLIENT_PREVENT_AOF_PROP|CLIENT_PREVENT_REPL_PROP)
#define CLIENT_PENDING_WRITE (1<<21) /* Client has output to send but a write
                                        handler is yet not installed. */
#define CLIENT_REPLY_OFF (1<<22)   /* Don't send replies to client. */
#define CLIENT_REPLY_SKIP_NEXT (1<<23)  /* Set CLIENT_REPLY_SKIP for next cmd */
#define CLIENT_REPLY_SKIP (1<<24)  /* Don't send just this reply. */
#define CLIENT_LUA_DEBUG (1<<25)  /* Run EVAL in debug mode. */
#define CLIENT_LUA_DEBUG_SYNC (1<<26)  /* EVAL debugging without fork() */
#define CLIENT_MODULE (1<<27) /* Non connected client used by some module. */

/* Command flags. Please check the command table defined in the redis.c file
 * for more information about the meaning of every flag. */
#define CMD_WRITE (1<<0)            /* "w" flag */
#define CMD_READONLY (1<<1)         /* "r" flag */
#define CMD_DENYOOM (1<<2)          /* "m" flag */
#define CMD_MODULE (1<<3)           /* Command exported by module. */
#define CMD_ADMIN (1<<4)            /* "a" flag */
#define CMD_PUBSUB (1<<5)           /* "p" flag */
#define CMD_NOSCRIPT (1<<6)         /* "s" flag */
#define CMD_RANDOM (1<<7)           /* "R" flag */
#define CMD_SORT_FOR_SCRIPT (1<<8)  /* "S" flag */
#define CMD_LOADING (1<<9)          /* "l" flag */
#define CMD_STALE (1<<10)           /* "t" flag */
#define CMD_SKIP_MONITOR (1<<11)    /* "M" flag */
#define CMD_ASKING (1<<12)          /* "k" flag */
#define CMD_FAST (1<<13)            /* "F" flag */
#define CMD_MODULE_GETKEYS (1<<14)  /* Use the modules getkeys interface. */
#define CMD_MODULE_NO_CLUSTER (1<<15) /* Deny on Redis Cluster. */

#define CMD_CALL_NONE 0
#define CMD_CALL_SLOWLOG (1<<0)
#define CMD_CALL_STATS (1<<1)
#define CMD_CALL_PROPAGATE_AOF (1<<2)
#define CMD_CALL_PROPAGATE_REPL (1<<3)
#define CMD_CALL_PROPAGATE (CMD_CALL_PROPAGATE_AOF|CMD_CALL_PROPAGATE_REPL)
#define CMD_CALL_FULL (CMD_CALL_SLOWLOG | CMD_CALL_STATS | CMD_CALL_PROPAGATE)

/* networking */
#define CONFIG_BINDADDR_MAX 16
#define CONFIG_DEFAULT_SERVER_PORT  6379
#define CONFIG_DEFAULT_DBNUM     16
#define LOG_MAX_LEN    1024 /* Default maximum length of syslog messages */ 
#define CONFIG_DEFAULT_MAX_CLIENTS 10000
#define CONFIG_DEFAULT_PROTO_MAX_BULK_LEN (512ll*1024*1024) /* Bulk request max size */

#define PROTO_REPLY_CHUNK_BYTES (16*1024) /* 16k output buffer */

/* Keyspace changes notification classes. Every class is associated with a
 * character for configuration purposes. */
#define NOTIFY_KEYSPACE (1<<0)    /* K */
#define NOTIFY_KEYEVENT (1<<1)    /* E */
#define NOTIFY_GENERIC (1<<2)     /* g */
#define NOTIFY_STRING (1<<3)      /* $ */
#define NOTIFY_LIST (1<<4)        /* l */
#define NOTIFY_SET (1<<5)         /* s */
#define NOTIFY_HASH (1<<6)        /* h */
#define NOTIFY_ZSET (1<<7)        /* z */
#define NOTIFY_EXPIRED (1<<8)     /* x */
#define NOTIFY_EVICTED (1<<9)     /* e */
#define NOTIFY_ALL (NOTIFY_GENERIC | NOTIFY_STRING | NOTIFY_LIST | NOTIFY_SET | NOTIFY_HASH | NOTIFY_ZSET | NOTIFY_EXPIRED | NOTIFY_EVICTED) /* A flag */


/* Log levels */
#define LL_DEBUG 0
#define LL_VERBOSE 1
#define LL_NOTICE 2
#define LL_WARNING 3
#define LL_RAW (1<<10) /* Modifier to log without timestamp */

#define OBJ_ENCODING_RAW 0     /* Raw representation */
#define OBJ_ENCODING_INT 1     /* Encoded as integer */
#define OBJ_ENCODING_HT 2      /* Encoded as hash table */
#define OBJ_ENCODING_ZIPMAP 3  /* Encoded as zipmap */
#define OBJ_ENCODING_LINKEDLIST 4 /* No longer used: old list encoding. */
#define OBJ_ENCODING_ZIPLIST 5 /* Encoded as ziplist */
#define OBJ_ENCODING_INTSET 6  /* Encoded as intset */
#define OBJ_ENCODING_SKIPLIST 7  /* Encoded as skiplist */
#define OBJ_ENCODING_EMBSTR 8  /* Embedded sds string encoding */
#define OBJ_ENCODING_QUICKLIST 9 /* Encoded as linked list of ziplists */

#define LRU_BITS 24
#define OBJ_SHARED_REFCOUNT INT_MAX

/* The actual Redis Object */
#define OBJ_STRING 0
#define OBJ_LIST 1
#define OBJ_SET 2
#define OBJ_ZSET 3
#define OBJ_HASH 4

#define serverAssert(_e) ((_e)?(void)0 : (_exit(1)))
#define serverAssertWithInfo(_c,_o,_e) ((_e)?(void)0 : (_serverAssertWithInfo(_c,_o,#_e,__FILE__,__LINE__),_exit(1)))


struct client;
struct redisCommand;

/**
 * @brief something worthy noticed is bit filed, redisObject:type following only uses 4 bit 
 * 
 */

typedef struct redisObject {
    unsigned type:4;
    unsigned encoding:4;
    int refcount;
    void *ptr;
} robj;

typedef struct redisDb {
    dict *dict;             /* The keyspace for this DB */
    dict *expires;          /* Timeout of keys with a timeout set */
    //dict *blocking_keys;    /* ke
    //dict *ready_keys;
    //dict *watched_keys;
    int id;
    long long avg_ttl;
} redisDb;


typedef void redisCommandProc(struct client *c);
typedef int *redisGetKeysProc(struct redisCommand *cmd, robj **argv, int argc, int *numkeys);
struct redisCommand {
    char *name;
    redisCommandProc *proc;
    int arity;
    char *sflags; /* Flags as string representation, one char per flag. */
    int flags;    /* The actual flags, obtained from the 'sflags' field. */
    /* Use a function to determine keys arguments in a command line.
     * Used for Redis Cluster redirect. */
    redisGetKeysProc *getkeys_proc;
    /* What keys should be loaded in background when calling this command? */
    int firstkey; /* The first argument that's a key (0 = no keys) */
    int lastkey;  /* The last argument that's a key */
    int keystep;  /* The step between first and last key */
    long long microseconds; /* total execution time */
    long long calls;/*total execution count */
};

struct sharedObjectsStruct {
    robj *crlf, *ok, *err, *emptybulk, *czero, *cone, *cnegone, *pong, *space,
    *colon, *nullbulk, *nullmultibulk, *queued,
    *emptymultibulk, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,
    *outofrangeerr, *noscripterr, *loadingerr, *slowscripterr, *bgsaveerr,
    *masterdownerr, *roslaveerr, *execaborterr, *noautherr, *noreplicaserr,
    *busykeyerr, *oomerr, *plus, *messagebulk, *pmessagebulk, *subscribebulk,
    *unsubscribebulk, *psubscribebulk, *punsubscribebulk, *del, *unlink,
    *rpop, *lpop, *lpush, *rpoplpush, *emptyscan,
    *select[PROTO_SHARED_SELECT_CMDS],
    *integers[OBJ_SHARED_INTEGERS],
    *mbulkhdr[OBJ_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
    *bulkhdr[OBJ_SHARED_BULKHDR_LEN];  /* "$<value>\r\n" */
    sds minstring, maxstring;
};

typedef struct client {
    uint64_t id;            /* Client incremental unique ID. */
    int fd;                 /* Client socket. */
    redisDb *db;            /* Pointer to currently SELECTed DB. */
    robj *name;             /* As set by CLIENT SETNAME. */
    sds querybuf;           /* Buffer we use to accumulate client queries. */
    int argc;               /* Num of arguments of current command. */
    robj **argv;            /* Arguments of current command. */
    struct redisCommand *cmd, *lastcmd;  /* Last command executed. */
    int reqtype;            /* Request protocol type: PROTO_REQ_* */
    int multibulklen;       /* Number of multi bulk arguments left to read. */
    long bulklen;           /* Length of bulk argument in multi bulk request. */
    list *reply;            /* List of reply objects to send to the client. */
    unsigned long long reply_bytes; /* Tot bytes of objects in reply list. */
    size_t sentlen;         /* Amount of bytes already sent in the current
                               buffer or object being sent. */
    time_t ctime;           /* Client creation time. */
    time_t lastinteraction; /* Time of the last interaction, used for timeout */
    int flags;              /* Client flags: CLIENT_* macros. */
    listNode *client_list_node;

    dict *pubsub_channels;


    /* Response buffer */
    int bufpos;             /* offset where data will be put in */ 
    char buf[PROTO_REPLY_CHUNK_BYTES];  // if the buf is not big enough ,then use the reply
} client;

typedef struct nadisServer {
    pid_t pid;                      /* Main process pid */
    redisDb *db;
    dict *commands;
    aeEventLoop *el;

    // networking
    int port;                       /* Tcp listening port */
    int tcp_backlog;
    long long dirty;
    int cronloops;              /* Number of times the cron function run */
    int hz; 
    int active_expire_enabled;      /* Can be disabled for testing purposes. */
    int stat_expired_time_cap_reached_count;
    int ipfd[CONFIG_BINDADDR_MAX];  /* TCP socket file descriptors */
    int ipfd_count;                 /* Used slots in ipfd[] */
    char *bindaddr[CONFIG_BINDADDR_MAX]; /* Addresses we should bind to */
    int bindaddr_count;             /* Number of addresses in server.bindaddr[] */
    char neterr[ANET_ERR_LEN];      /* Error buffer for anet.c */
    list *clients;              /* List of active clients */
    list *clients_pending_write; /* There is to write or install handler. */
    list *clients_to_close;
    client *current_client;     /* current client, only used on crash report */

    dict *pubsub_channels;
    int notify_keyspace_events; /* Events to propagate via Pub/Sub. This is an
                                   xor of NOTIFY_... flags. */

    /* stats */
    double stat_expired_stale_perc; /* Percentage of keys probably expired */

    unsigned int maxclients;            /* Max number of simultaneous clients */
    /* Limits */
    uint64_t next_client_id;        
    long long proto_max_bulk_len;

    int dbnum;                      /* Total number of configured DBs */
    int verbosity;                  /* Loglevel in redis.conf */
} nadisServer;



long long ustime(void);
long long mstime(void);
void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask); 
void serverLog(int level, const char *fmt, ...);
void serverPanic(const char*str);
int handleClientsWithPendingWrites(void);


extern nadisServer server;
extern struct sharedObjectsStruct shared;

extern dictType objectKeyPointerValueDictType;
extern dictType setDictType;
extern dictType zsetDictType;
extern dictType clusterNodesDictType;
extern dictType clusterNodesBlackListDictType;
extern dictType dbDictType;
extern dictType shaScriptObjectDictType;
extern double R_Zero, R_PosInf, R_NegInf, R_Nan;
extern dictType hashDictType;
extern dictType replScriptCacheDictType;
extern dictType keyptrDictType;
extern dictType modulesDictType;

void resetClient(client *c);

void populateCommandTable(void);
int processCommand(client *c);
struct redisCommand *lookupCommand(sds name);
void getCommand(client *c);
void setCommand(client *c);
robj *lookupKeyWrite(redisDb *db, robj *key);

void _serverAssert(const char *estr, const char *file, int line);
void _serverAssertWithInfo(const client *c, const robj *o, const char *estr, const char *file, int line);

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData);
void addReplyErrorLength(client *c, const char *s, size_t len);
void addReplyError(client *c, const char *err);
void addReplyLongLong(client *c, long long ll);
void addReply(client *client, robj *obj);
void addReplyBulk(client *c, robj *obj);
robj *lookupKeyReadOrReply(client *c, robj *key, robj *reply);

void rewriteClientCommandVector(client *c, int argc, ...);
robj *createObject(int type, void *ptr);
robj *createEmbeddedStringObject(const char *ptr, size_t len);
robj *createRawStringObject(const char *ptr, size_t len);
robj *createStringObject(const char *ptr, size_t len);
void decrRefCount(robj *o);
void incrRefCount(robj *o);

void expireCommand(client *c);
void activeExpireCycle(int type);
int expireIfNeeded(redisDb *db, robj *key);
void setExpire(client *c, redisDb *db, robj *key, long long when);
long long getExpire(redisDb *db, robj *key);
#define LOOKUP_NONE 0
#define LOOKUP_NOTOUCH (1<<0)
int selectDb(client *c, int id);
void signalModifiedKey(redisDb *db, robj *key);
void setKey(redisDb *db, robj *key, robj *val);
int dbSyncDelete(redisDb *db, robj *key);
/* Return the UNIX time in microseconds */

int getLongLongFromObjectOrReply(client *c, robj *o, long long *target, const char *msg);
robj *getDecodedObject(robj *o);
void publishCommand(client *c);
void subscribeCommand(client *c);
void unsubscribeCommand(client *c);
#define sdsEncodedObject(objptr) (objptr->encoding == OBJ_ENCODING_RAW || objptr->encoding == OBJ_ENCODING_EMBSTR)


int keyspaceEventsStringToFlags(char *classes);
sds keyspaceEventsFlagsToString(int flags);
void notifyKeyspaceEvent(int type, char *event, robj *key, int dbid);

int pubsubPublishMessage(robj *channel, robj *message);



#endif
