
#include "server.h"

// void bugReportStart(void) {
//     if (server.bug_report_start == 0) {
//         serverLogRaw(LL_WARNING|LL_RAW,
//         "\n\n=== REDIS BUG REPORT START: Cut & paste starting from here ===\n");
//         server.bug_report_start = 1;
//     }
// }

void _serverAssertPrintClientInfo(const client *c) {
    int j;

    //bugReportStart();
    serverLog(LL_WARNING,"=== ASSERTION FAILED CLIENT CONTEXT ===");
    serverLog(LL_WARNING,"client->flags = %d", c->flags);
    serverLog(LL_WARNING,"client->fd = %d", c->fd);
    serverLog(LL_WARNING,"client->argc = %d", c->argc);
    for (j=0; j < c->argc; j++) {
        char buf[128];
        char *arg;

        if (c->argv[j]->type == OBJ_STRING && sdsEncodedObject(c->argv[j])) {
            arg = (char*) c->argv[j]->ptr;
        } else {
            snprintf(buf,sizeof(buf),"Object type: %u, encoding: %u",
                c->argv[j]->type, c->argv[j]->encoding);
            arg = buf;
        }
        serverLog(LL_WARNING,"client->argv[%d] = \"%s\" (refcount: %d)",
            j, arg, c->argv[j]->refcount);
    }
}

void serverLogObjectDebugInfo(const robj *o) {
    serverLog(LL_WARNING,"Object type: %d", o->type);
    serverLog(LL_WARNING,"Object encoding: %d", o->encoding);
    serverLog(LL_WARNING,"Object refcount: %d", o->refcount);
    if (o->type == OBJ_STRING && sdsEncodedObject(o)) {
        serverLog(LL_WARNING,"Object raw string len: %zu", sdslen(o->ptr));
        if (sdslen(o->ptr) < 4096) {
            sds repr = sdscatrepr(sdsempty(),o->ptr,sdslen(o->ptr));
            serverLog(LL_WARNING,"Object raw string content: %s", repr);
            sdsfree(repr);
        }
    } else if (o->type == OBJ_LIST) {
        //serverLog(LL_WARNING,"List length: %d", (int) listTypeLength(o)); todo.2
    } else if (o->type == OBJ_SET) {
        //serverLog(LL_WARNING,"Set size: %d", (int) setTypeSize(o)); todo.2
    } else if (o->type == OBJ_HASH) {
        //serverLog(LL_WARNING,"Hash size: %d", (int) hashTypeLength(o)); todo.2
    }
    // } else if (o->type == OBJ_ZSET) {
    //     serverLog(LL_WARNING,"Sorted set size: %d", (int) zsetLength(o));
    //     if (o->encoding == OBJ_ENCODING_SKIPLIST)
    //         serverLog(LL_WARNING,"Skiplist level: %d", (int) ((const zset*)o->ptr)->zsl->level);
    // }
}

void _serverAssertPrintObject(const robj *o) {
    //bugReportStart();
    serverLog(LL_WARNING,"=== ASSERTION FAILED OBJECT CONTEXT ===");
    serverLogObjectDebugInfo(o);
}

void _serverAssert(const char *estr, const char *file, int line) {
    //bugReportStart();
    serverLog(LL_WARNING,"=== ASSERTION FAILED ===");
    serverLog(LL_WARNING,"==> %s:%d '%s' is not true",file,line,estr);
#ifdef HAVE_BACKTRACE
    server.assert_failed = estr;
    server.assert_file = file;
    server.assert_line = line;
    serverLog(LL_WARNING,"(forcing SIGSEGV to print the bug report.)");
#endif
    *((char*)-1) = 'x';
}

void _serverAssertWithInfo(const client *c, const robj *o, const char *estr, const char *file, int line) {
    if (c) _serverAssertPrintClientInfo(c);
    if (o) _serverAssertPrintObject(o);
    _serverAssert(estr,file,line);
}