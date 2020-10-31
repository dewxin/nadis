
/**
 * @file networking.c 
 * @author your name (you@domain.com)
 * @brief  you could see it as a part of the server.c
 * @version 0.1
 * @date 2020-10-22
 * 
 * @copyright Copyright (c) 2020
 * 
 */

#include "server.h"

#define MAX_ACCEPTS_PER_CALL 1000
#define NET_IP_STR_LEN 46 /* INET6_ADDRSTRLEN is 46, but we need to be sure */
#define PROTO_IOBUF_LEN         (1024*16)  /* Generic I/O buffer size */
#define PROTO_MBULK_BIG_ARG     (1024*32)
#define MAX_ACCEPTS_PER_CALL 1000
#define PROTO_INLINE_MAX_SIZE   (1024*64) /* Max size of inline reads */


void addReplyBulkLen(client *c, robj *obj) {
    size_t len;

    if (sdsEncodedObject(obj)) {
        len = sdslen(obj->ptr);
    } else {
        long n = (long)obj->ptr;

        /* Compute how many bytes will take this integer as a radix 10 string */
        len = 1;
        if (n < 0) {
            len++;
            n = -n;
        }
        while((n = n/10) != 0) {
            len++;
        }
    }

    if (len < OBJ_SHARED_BULKHDR_LEN)
        addReply(c,shared.bulkhdr[len]);
    // else
    //     addReplyLongLongWithPrefix(c,len,'$'); todo.1
}

void addReplyBulk(client *c, robj *obj) {
    addReplyBulkLen(c,obj);
    addReply(c,obj);
    addReply(c,shared.crlf);
}
int _addReplyToBuffer(client *c, const char *replyStr, size_t len) {
    size_t available = sizeof(c->buf)-c->bufpos;

    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return C_OK;

    /* If there already are entries in the reply list, we cannot
     * add anything more to the static buffer. */
    if (listLength(c->reply) > 0) return C_ERR;

    /* Check that the buffer has enough space available for this string. */
    if (len > available) return C_ERR;

    memcpy(c->buf+c->bufpos,replyStr,len);
    c->bufpos+=len;
    return C_OK;
}

void freeClientAsync(client *c) {
    if (c->flags & CLIENT_CLOSE_ASAP || c->flags & CLIENT_LUA) return;
    c->flags |= CLIENT_CLOSE_ASAP;
    listAddNodeTail(server.clients_to_close,c);
}
int clientHasPendingReplies(client *c) {
    return c->bufpos || listLength(c->reply);
}

static void freeClientArgv(client *c) {
    int j;
    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    c->argc = 0;
    c->cmd = NULL;
}

void unlinkClient(client *c) {
    listNode *ln;

    /* If this is marked as current client unset it. */
    if (server.current_client == c) server.current_client = NULL;

    /* Certain operations must be done only if the client has an active socket.
     * If the client was already unlinked or if it's a "fake client" the
     * fd is already set to -1. */
    if (c->fd != -1) {
        /* Remove from the list of active clients. */
        if (c->client_list_node) {
            listDelNode(server.clients,c->client_list_node);
            c->client_list_node = NULL;
        }

        /* Unregister async I/O handlers and close the socket. */
        aeDeleteFileEvent(server.el,c->fd,AE_READABLE);
        aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
        close(c->fd);
        c->fd = -1;
    }

    /* Remove from the list of pending writes if needed. */
    if (c->flags & CLIENT_PENDING_WRITE) {
        ln = listSearchKey(server.clients_pending_write,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_pending_write,ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
    }

    /* When client was just unblocked because of a blocking operation,
     * remove it from the list of unblocked clients. */
    // if (c->flags & CLIENT_UNBLOCKED) { todo
    //     ln = listSearchKey(server.unblocked_clients,c);
    //     serverAssert(ln != NULL);
    //     listDelNode(server.unblocked_clients,ln);
    //     c->flags &= ~CLIENT_UNBLOCKED;
    // }
}

void freeClient(client *c) {
    listNode *ln;

    /* If it is our master that's beging disconnected we should make sure
     * to cache the state to try a partial resynchronization later.
     *
     * Note that before doing this we make sure that the client is not in
     * some unexpected state, by checking its flags. */
    // if (server.master && c->flags & CLIENT_MASTER) {
    //     serverLog(LL_WARNING,"Connection with master lost.");
    //     if (!(c->flags & (CLIENT_CLOSE_AFTER_REPLY|
    //                       CLIENT_CLOSE_ASAP|
    //                       CLIENT_BLOCKED|
    //                       CLIENT_UNBLOCKED)))
    //     {
    //         replicationCacheMaster(c);
    //         return;
    //     }
    // }

    /* Log link disconnection with slave */
    // if ((c->flags & CLIENT_SLAVE) && !(c->flags & CLIENT_MONITOR)) {
    //     serverLog(LL_WARNING,"Connection with slave %s lost.",
    //         replicationGetSlaveName(c));
    // }

    /* Free the query buffer */
    sdsfree(c->querybuf);
    //sdsfree(c->pending_querybuf); todo
    c->querybuf = NULL;

    /* Deallocate structures used to block on blocking ops. */
    // if (c->flags & CLIENT_BLOCKED) unblockClient(c); todo
    //dictRelease(c->bpop.keys);

    /* UNWATCH all the keys */
    //unwatchAllKeys(c); todo
    //listRelease(c->watched_keys);

    /* Unsubscribe from all the pubsub channels */
    // pubsubUnsubscribeAllChannels(c,0); todo
    // pubsubUnsubscribeAllPatterns(c,0);
    // dictRelease(c->pubsub_channels);
    // listRelease(c->pubsub_patterns);

    /* Free data structures. */
    listRelease(c->reply);
    freeClientArgv(c);

    /* Unlink the client: this will close the socket, remove the I/O
     * handlers, and remove references of the client from different
     * places where active clients may be referenced. */
    unlinkClient(c);

    /* Master/slave cleanup Case 1:
     * we lost the connection with a slave. */
    // if (c->flags & CLIENT_SLAVE) {
    //     if (c->replstate == SLAVE_STATE_SEND_BULK) {
    //         if (c->repldbfd != -1) close(c->repldbfd);
    //         if (c->replpreamble) sdsfree(c->replpreamble);
    //     }
    //     list *l = (c->flags & CLIENT_MONITOR) ? server.monitors : server.slaves;
    //     ln = listSearchKey(l,c);
    //     serverAssert(ln != NULL);
    //     listDelNode(l,ln);
    //     /* We need to remember the time when we started to have zero
    //      * attached slaves, as after some time we'll free the replication
    //      * backlog. */
    //     // if (c->flags & CLIENT_SLAVE && listLength(server.slaves) == 0) todo
    //     //     server.repl_no_slaves_since = server.unixtime;
    //     // refreshGoodSlavesCount();
    // }

    /* Master/slave cleanup Case 2:
     * we lost the connection with the master. */
    //if (c->flags & CLIENT_MASTER) replicationHandleMasterDisconnection(); todo

    /* If this client was scheduled for async freeing we need to remove it
     * from the queue. */
    if (c->flags & CLIENT_CLOSE_ASAP) {
        ln = listSearchKey(server.clients_to_close,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_to_close,ln);
    }

    /* Release other dynamically allocated client structure fields,
     * and finally release the client structure itself. */
    if (c->name) decrRefCount(c->name);
    zfree(c->argv);
    //freeClientMultiState(c);
    //sdsfree(c->peerid);
    zfree(c);
}
// void asyncCloseClientOnOutputBufferLimitReached(client *c) {
//     if (c->fd == -1) return; /* It is unsafe to free fake clients. */
//     //serverAssert(c->reply_bytes < SIZE_MAX-(1024*64));
//     if (c->reply_bytes == 0 || c->flags & CLIENT_CLOSE_ASAP) return;
//     //if (checkClientOutputBufferLimits(c)) { todo
//         sds client = catClientInfoString(sdsempty(),c);

//         freeClientAsync(c);
//         serverLog(LL_WARNING,"Client %s scheduled to be closed ASAP for overcoming of output buffer limits.", client);
//         sdsfree(client);
//     //}
// }

void _addReplyObjectToList(client *c, robj *o) {
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    if (listLength(c->reply) == 0) {
        sds s = sdsdup(o->ptr);
        listAddNodeTail(c->reply,s);
        c->reply_bytes += sdslen(s);
    } else {
        listNode *ln = listLast(c->reply);
        sds tail = listNodeValue(ln);

        /* Append to this object when possible. If tail == NULL it was
         * set via addDeferredMultiBulkLength(). */
        if (tail && sdslen(tail)+sdslen(o->ptr) <= PROTO_REPLY_CHUNK_BYTES) {
            tail = sdscatsds(tail,o->ptr);
            listNodeValue(ln) = tail;
            c->reply_bytes += sdslen(o->ptr);
        } else {
            sds s = sdsdup(o->ptr);
            listAddNodeTail(c->reply,s);
            c->reply_bytes += sdslen(s);
        }
    }
    //asyncCloseClientOnOutputBufferLimitReached(c);
}


int prepareClientToWrite(client *c) {
    /* If it's the Lua client we always return ok without installing any
     * handler since there is no socket at all. */
    if (c->flags & (CLIENT_LUA|CLIENT_MODULE)) return C_OK;

    /* CLIENT REPLY OFF / SKIP handling: don't send replies. */
    if (c->flags & (CLIENT_REPLY_OFF|CLIENT_REPLY_SKIP)) return C_ERR;

    /* Masters don't receive replies, unless CLIENT_MASTER_FORCE_REPLY flag
     * is set. */
    if ((c->flags & CLIENT_MASTER) &&
        !(c->flags & CLIENT_MASTER_FORCE_REPLY)) return C_ERR;

    if (c->fd <= 0) return C_ERR; /* Fake client for AOF loading. */

    /* Schedule the client to write the output buffers to the socket only
     * if not already done (there were no pending writes already and the client
     * was yet not flagged), and, for slaves, if the slave can actually
     * receive writes at this stage. */
    if (!clientHasPendingReplies(c) &&
        !(c->flags & CLIENT_PENDING_WRITE))// &&
        //(c->replstate == REPL_STATE_NONE ||
        // (c->replstate == SLAVE_STATE_ONLINE && !c->repl_put_online_on_ack)))
    {
        /* Here instead of installing the write handler, we just flag the
         * client and put it into a list of clients that have something
         * to write to the socket. This way before re-entering the event
         * loop, we can try to directly write to the client sockets avoiding
         * a system call. We'll only really install the write handler if
         * we'll not be able to write the whole reply at once. */
        c->flags |= CLIENT_PENDING_WRITE;
        listAddNodeHead(server.clients_pending_write,c);
    }

    /* Authorize the caller to queue in the output buffer of this client. */
    return C_OK;
}

/**
 * @brief add reply obj to the client buffer,
 * 
 * @param client 
 * @param obj   reply object, encoding can only be string or int
 */
void addReply(client *client, robj *obj) {
    if (prepareClientToWrite(client) != C_OK) return; 

    /* This is an important place where we can avoid copy-on-write
     * when there is a saving child running, avoiding touching the
     * refcount field of the object if it's not needed.
     *
     * If the encoding is RAW and there is room in the static buffer
     * we'll be able to send the object to the client without
     * messing with its page. */
    if (sdsEncodedObject(obj)) {
        if (_addReplyToBuffer(client,obj->ptr,sdslen(obj->ptr)) != C_OK)
            _addReplyObjectToList(client,obj);
    } else if (obj->encoding == OBJ_ENCODING_INT) {
        /* Optimization: if there is room in the static buffer for 32 bytes
         * (more than the max chars a 64 bit integer can take as string) we
         * avoid decoding the object and go for the lower level approach. */
        if (listLength(client->reply) == 0 && (sizeof(client->buf) - client->bufpos) >= 32) {
            char buf[32];
            int len;

            len = ll2string(buf,sizeof(buf),(long)obj->ptr);
            if (_addReplyToBuffer(client,buf,len) == C_OK)
                return;
            /* else... continue with the normal code path, but should never
             * happen actually since we verified there is room. */
        }
        obj = getDecodedObject(obj);
        if (_addReplyToBuffer(client,obj->ptr,sdslen(obj->ptr)) != C_OK)
            _addReplyObjectToList(client,obj);
        decrRefCount(obj);
    } else {
        // serverPanic("Wrong obj->encoding in addReply()");
        serverLog(LL_WARNING, "Wrong obj->encoding in addReply()");
    }
}

void _addReplyStringToList(client *c, const char *s, size_t len) {
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    if (listLength(c->reply) == 0) {
        sds node = sdsnewlen(s,len);
        listAddNodeTail(c->reply,node);
        c->reply_bytes += len;
    } else {
        listNode *ln = listLast(c->reply);
        sds tail = listNodeValue(ln);

        /* Append to this object when possible. If tail == NULL it was
         * set via addDeferredMultiBulkLength(). */
        if (tail && sdslen(tail)+len <= PROTO_REPLY_CHUNK_BYTES) {
            tail = sdscatlen(tail,s,len);
            listNodeValue(ln) = tail;
            c->reply_bytes += len;
        } else {
            sds node = sdsnewlen(s,len);
            listAddNodeTail(c->reply,node);
            c->reply_bytes += len;
        }
    }
   // asyncCloseClientOnOutputBufferLimitReached(c); todo
}

void addReplyString(client *c, const char *s, size_t len) {
    if (prepareClientToWrite(c) != C_OK) return;
    if (_addReplyToBuffer(c,s,len) != C_OK)
        _addReplyStringToList(c,s,len);
}

void addReplyLongLongWithPrefix(client *c, long long ll, char prefix) {
    char buf[128];
    int len;

    /* Things like $3\r\n or *2\r\n are emitted very often by the protocol
     * so we have a few shared objects to use if the integer is small
     * like it is most of the times. */
    if (prefix == '*' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        addReply(c,shared.mbulkhdr[ll]);
        return;
    } else if (prefix == '$' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        addReply(c,shared.bulkhdr[ll]);
        return;
    }

    buf[0] = prefix;
    len = ll2string(buf+1,sizeof(buf)-1,ll);
    buf[len+1] = '\r';
    buf[len+2] = '\n';
    addReplyString(c,buf,len+3);
}

void addReplyErrorLength(client *c, const char *s, size_t len) {
    addReplyString(c,"-ERR ",5);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
    if (c->flags & (CLIENT_MASTER|CLIENT_SLAVE)) {
        char* to = c->flags & CLIENT_MASTER? "master": "slave";
        char* from = c->flags & CLIENT_MASTER? "slave": "master";
        char *cmdname = c->lastcmd ? c->lastcmd->name : "<unknown>";
        serverLog(LL_WARNING,"== CRITICAL == This %s is sending an error "
                             "to its %s: '%s' after processing the command "
                             "'%s'", from, to, s, cmdname);
    }
}

void addReplyError(client *c, const char *err) {
    addReplyErrorLength(c,err,strlen(err));
}

void addReplyLongLong(client *c, long long ll) {
    if (ll == 0)
        addReply(c,shared.czero);
    else if (ll == 1)
        addReply(c,shared.cone);
    else
        addReplyLongLongWithPrefix(c,ll,':');
}

/* Client.reply list dup and free methods. */
void *dupClientReplyValue(void *o) {
    return sdsdup(o);
}

void freeClientReplyValue(void *o) {
    sdsfree(o);

}



void linkClient(client *c) {
    listAddNodeTail(server.clients,c);
    /* Note that we remember the linked list node where the client is stored,
     * this way removing the client in unlinkClient() will not require
     * a linear scan, but just a constant time operation. */
    c->client_list_node = listLast(server.clients);
}

/* Write data in output buffers to client. Return C_OK if the client
 * is still valid after the call, C_ERR if it was freed. */
int writeToClient(int fd, client *c, int handler_installed) {
    ssize_t nwritten = 0, totwritten = 0;
    size_t objlen;
    sds o;

    while(clientHasPendingReplies(c)) {
        if (c->bufpos > 0) {
            nwritten = write(fd,c->buf+c->sentlen,c->bufpos-c->sentlen);
            if (nwritten <= 0) break;
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If the buffer was sent, set bufpos to zero to continue with
             * the remainder of the reply. */
            if ((int)c->sentlen == c->bufpos) {
                c->bufpos = 0;
                c->sentlen = 0;
            }
        } else {
            o = listNodeValue(listFirst(c->reply));
            objlen = sdslen(o);

            if (objlen == 0) {
                listDelNode(c->reply,listFirst(c->reply));
                continue;
            }

            nwritten = write(fd, o + c->sentlen, objlen - c->sentlen);
            if (nwritten <= 0) break;
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If we fully sent the object on head go to the next one */
            if (c->sentlen == objlen) {
                listDelNode(c->reply,listFirst(c->reply));
                c->sentlen = 0;
                c->reply_bytes -= objlen;
                /* If there are no longer objects in the list, we expect
                 * the count of reply bytes to be exactly zero. */
                // if (listLength(c->reply) == 0)
                //     serverAssert(c->reply_bytes == 0);
            }
        }
        /* Note that we avoid to send more than NET_MAX_WRITES_PER_EVENT
         * bytes, in a single threaded server it's a good idea to serve
         * other clients as well, even if a very large request comes from
         * super fast link that is always able to accept data (in real world
         * scenario think about 'KEYS *' against the loopback interface).
         *
         * However if we are over the maxmemory limit we ignore that and
         * just deliver as much data as it is possible to deliver.
         *
         * Moreover, we also send as much as possible if the client is
         * a slave (otherwise, on high-speed traffic, the replication
         * buffer will grow indefinitely) */
        // if (totwritten > NET_MAX_WRITES_PER_EVENT &&
        //     (//server.maxmemory == 0 || todo
        //      zmalloc_used_memory() < server.maxmemory) &&
        //     !(c->flags & CLIENT_SLAVE)) break;
    }
    //server.stat_net_output_bytes += totwritten;
    if (nwritten == -1) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } else {
            serverLog(LL_VERBOSE,
                "Error writing to client: %s", strerror(errno));
            freeClient(c);
            return C_ERR;
        }
    }
    if (totwritten > 0) {
        /* For clients representing masters we don't count sending data
         * as an interaction, since we always send REPLCONF ACK commands
         * that take some time to just fill the socket output buffer.
         * We just rely on data / pings received for timeout detection. */
        // if (!(c->flags & CLIENT_MASTER)) c->lastinteraction = server.unixtime; todo.1
    }
    if (!clientHasPendingReplies(c)) {
        c->sentlen = 0;
        if (handler_installed) aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);

        /* Close connection after entire reply has been sent. */
        if (c->flags & CLIENT_CLOSE_AFTER_REPLY) {
            freeClient(c);
            return C_ERR;
        }
    }
    return C_OK;
}

/* Write event handler. Just send data to the client. */
void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask) {

    writeToClient(fd,privdata,1);
}



int handleClientsWithPendingWrites(void) {
    listIter li;
    listNode *ln;
    int processed = listLength(server.clients_pending_write);

    listRewind(server.clients_pending_write,&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
        listDelNode(server.clients_pending_write,ln);

        /* Try to write buffers to the client socket. */
        if (writeToClient(c->fd,c,0) == C_ERR) continue;

        /* If after the synchronous writes above we still have data to
         * output to the client, we need to install the writable handler. */
        if (clientHasPendingReplies(c)) {
             int ae_flags = AE_WRITABLE; 
            // /* For the fsync=always policy, we want that a given FD is never
            //  * served for reading and writing in the same event loop iteration,
            //  * so that in the middle of receiving the query, and serving it
            //  * to the client, we'll call beforeSleep() that will do the
            //  * actual fsync of AOF to disk. AE_BARRIER ensures that. */

            if (aeCreateFileEvent(server.el, c->fd, ae_flags,
                sendReplyToClient, c) == AE_ERR)
            {
                    freeClientAsync(c);
            }
        }
    }
    return processed;
}

/* Process the query buffer for client 'c', setting up the client argument
 * vector for command execution. Returns C_OK if after running the function
 * the client has a well-formed ready to be processed command, otherwise
 * C_ERR if there is still to read more buffer to get the full command.
 * The function also returns C_ERR when there is a protocol error: in such a
 * case the client structure is setup to reply with the error and close
 * the connection.
 *
 * This function is called if processInputBuffer() detects that the next
 * command is in RESP format, so the first byte in the command is found
 * to be '*'. Otherwise for inline commands processInlineBuffer() is called. */
int processMultibulkBuffer(client *c) {
    char *newline = NULL;
    long pos = 0;
    int ok;
    long long ll; //line count to be read

    if (c->multibulklen == 0) {
        /* The client should have been reset */
        //serverAssertWithInfo(c,NULL,c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(c->querybuf,'\r');
        if (newline == NULL) {
            if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
                // addReplyError(c,"Protocol error: too big mbulk count string"); todo
                // setProtocolError("too big mbulk count string",c,0);
            }
            return C_ERR;
        }

        /* Buffer should also contain \n */
        if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
            return C_ERR;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        //serverAssertWithInfo(c,NULL,c->querybuf[0] == '*');
        ok = string2ll(c->querybuf+1,newline-(c->querybuf+1),&ll);
        if (!ok || ll > 1024*1024) {
            // addReplyError(c,"Protocol error: invalid multibulk length");
            // setProtocolError("invalid mbulk count",c,pos);
            return C_ERR;
        }

        pos = (newline-c->querybuf)+2;
        if (ll <= 0) {
            sdsrange(c->querybuf,pos,-1);
            return C_OK;
        }

        c->multibulklen = ll;

        /* Setup argv array on client structure */
        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(robj*)*c->multibulklen);
    }

    //serverAssertWithInfo(c,NULL,c->multibulklen > 0);
    while(c->multibulklen) {
        /* Read bulk length if unknown */
        if (c->bulklen == -1) {
            newline = strchr(c->querybuf+pos,'\r');
            if (newline == NULL) {
                if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
                    // addReplyError(c,
                    //     "Protocol error: too big bulk count string");
                    // setProtocolError("too big bulk count string",c,0);
                    return C_ERR;
                }
                break;
            }

            /* Buffer should also contain \n */
            if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
                break;

            if (c->querybuf[pos] != '$') {
                // addReplyErrorFormat(c, todo
                //     "Protocol error: expected '$', got '%c'",
                //     c->querybuf[pos]);
                // setProtocolError("expected $ but got something else",c,pos);
                return C_ERR;
            }

            ok = string2ll(c->querybuf+pos+1,newline-(c->querybuf+pos+1),&ll);
            if (!ok || ll < 0 || ll > server.proto_max_bulk_len) {
                // addReplyError(c,"Protocol error: invalid bulk length");
                // setProtocolError("invalid bulk length",c,pos);
                return C_ERR;
            }

            pos += newline-(c->querybuf+pos)+2;
            if (ll >= PROTO_MBULK_BIG_ARG) {
                size_t qblen;

                /* If we are going to read a large object from network
                 * try to make it likely that it will start at c->querybuf
                 * boundary so that we can optimize object creation
                 * avoiding a large copy of data. */
                sdsrange(c->querybuf,pos,-1);
                pos = 0;
                qblen = sdslen(c->querybuf);
                /* Hint the sds library about the amount of bytes this string is
                 * going to contain. */
                if (qblen < (size_t)ll+2)
                    c->querybuf = sdsMakeRoomFor(c->querybuf,ll+2-qblen);
            }
            c->bulklen = ll;
        }

        /* Read bulk argument */
        if (sdslen(c->querybuf)-pos < (size_t)(c->bulklen+2)) {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            /* Optimization: if the buffer contains JUST our bulk element
             * instead of creating a new object by *copying* the sds we
             * just use the current sds string. */
            if (pos == 0 &&
                c->bulklen >= PROTO_MBULK_BIG_ARG &&
                sdslen(c->querybuf) == (size_t)(c->bulklen+2))
            {
                c->argv[c->argc++] = createObject(OBJ_STRING,c->querybuf);
                sdsIncrLen(c->querybuf,-2); /* remove CRLF */
                /* Assume that if we saw a fat argument we'll see another one
                 * likely... */
                c->querybuf = sdsnewlen(NULL,c->bulklen+2);
                sdsclear(c->querybuf);
                pos = 0;
            } else {
                c->argv[c->argc++] =
                    createStringObject(c->querybuf+pos,c->bulklen);
                pos += c->bulklen+2;
            }
            c->bulklen = -1;
            c->multibulklen--;
        }
    }

    /* Trim to pos */
    if (pos) sdsrange(c->querybuf,pos,-1);

    /* We're done when c->multibulk == 0 */
    if (c->multibulklen == 0) return C_OK;

    /* Still not ready to process the command */
    return C_ERR;
}

/* Rewrite the command vector of the client. All the new objects ref count
 * is incremented. The old command vector is freed, and the old objects
 * ref count is decremented. */
void rewriteClientCommandVector(client *c, int argc, ...) {
    va_list ap;
    int j;
    robj **argv; /* The new argument vector */

    argv = zmalloc(sizeof(robj*)*argc);
    va_start(ap,argc);
    for (j = 0; j < argc; j++) {
        robj *a;

        a = va_arg(ap, robj*);
        argv[j] = a;
        incrRefCount(a);
    }
    /* We free the objects in the original vector at the end, so we are
     * sure that if the same objects are reused in the new vector the
     * refcount gets incremented before it gets decremented. */
    for (j = 0; j < c->argc; j++) decrRefCount(c->argv[j]);
    zfree(c->argv);
    /* Replace argv and argc with our new versions. */
    c->argv = argv;
    c->argc = argc;
    c->cmd = lookupCommand(c->argv[0]->ptr);
    serverAssertWithInfo(c,NULL,c->cmd != NULL);
    va_end(ap);
}


/* resetClient prepare the client to process the next command */
void resetClient(client *c) {
    //redisCommandProc *prevcmd = c->cmd ? c->cmd->proc : NULL; todo

    freeClientArgv(c);
    c->reqtype = 0;
    c->multibulklen = 0;
    c->bulklen = -1;

    /* We clear the ASKING flag as well if we are not inside a MULTI, and
     * if what we just executed is not the ASKING command itself. */
    // if (!(c->flags & CLIENT_MULTI) && prevcmd != askingCommand) todo
    //     c->flags &= ~CLIENT_ASKING;

    /* Remove the CLIENT_REPLY_SKIP flag if any so that the reply
     * to the next command will be sent, but set the flag if the command
     * we just processed was "CLIENT REPLY SKIP". */
    c->flags &= ~CLIENT_REPLY_SKIP;
    if (c->flags & CLIENT_REPLY_SKIP_NEXT) {
        c->flags |= CLIENT_REPLY_SKIP;
        c->flags &= ~CLIENT_REPLY_SKIP_NEXT;
    }
}

void processInputBuffer(client *c) {
    server.current_client = c; 
    /* Keep processing while there is something in the input buffer */
    while(sdslen(c->querybuf)) {
        /* Return if clients are paused. */
        // if (!(c->flags & CLIENT_SLAVE) && clientsArePaused()) break;

        /* Immediately abort if the client is in the middle of something. */
        //if (c->flags & CLIENT_BLOCKED) break;

        /* CLIENT_CLOSE_AFTER_REPLY closes the connection once the reply is
         * written to the client. Make sure to not let the reply grow after
         * this flag has been set (i.e. don't process more commands).
         *
         * The same applies for clients we want to terminate ASAP. */
        //if (c->flags & (CLIENT_CLOSE_AFTER_REPLY|CLIENT_CLOSE_ASAP)) break;

        /* Determine request type when unknown. */
        if (!c->reqtype) {
            if (c->querybuf[0] == '*') {
                c->reqtype = PROTO_REQ_MULTIBULK;
            } else {
                c->reqtype = PROTO_REQ_INLINE;
            }
        }
        serverLog(LL_WARNING, "client send messgage : %s", c->querybuf);

        if (c->reqtype == PROTO_REQ_INLINE) { 
            //if (processInlineBuffer(c) != C_OK) break;
            serverLog(LL_WARNING, "receive inline request");
        } else if (c->reqtype == PROTO_REQ_MULTIBULK) {
            if (processMultibulkBuffer(c) != C_OK) break;
        } else {
            serverLog(LL_WARNING, "Unkown request type");
        }

        // /* Multibulk processing could see a <= 0 length. */
        if (c->argc == 0) {
            resetClient(c); 
        } else {
            /* Only reset the client when the command was executed. */
            if (processCommand(c) == C_OK) { //todo
                // if (c->flags & CLIENT_MASTER && !(c->flags & CLIENT_MULTI)) {
                //     /* Update the applied replication offset of our master. */
                //     c->reploff = c->read_reploff - sdslen(c->querybuf);
                // }

                /* Don't reset the client structure for clients blocked in a
                 * module blocking command, so that the reply callback will
                 * still be able to access the client argv and argc field.
                 * The client will be reset in unblockClientFromModule(). */
                // if (!(c->flags & CLIENT_BLOCKED) || c->btype != BLOCKED_MODULE)
                    resetClient(c); 
            }
            /* freeMemoryIfNeeded may flush slave output buffers. This may
             * result into a slave, that may be the active client, to be
             * freed. */
            if (server.current_client == NULL) break;
        }
    }
    server.current_client = NULL;
}

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    client *c = (client*) privdata;
    int nread, readlen;
    size_t qblen; //queryBufferLength
    // UNUSED(el);
    // UNUSED(mask);

    readlen = PROTO_IOBUF_LEN;
    /* If this is a multi bulk request, and we are processing a bulk reply
     * that is large enough, try to maximize the probability that the query
     * buffer contains exactly the SDS string representing the object, even
     * at the risk of requiring more read(2) calls. This way the function
     * processMultiBulkBuffer() can avoid copying buffers to create the
     * Redis Object representing the argument. */
    if (c->reqtype == PROTO_REQ_MULTIBULK && c->multibulklen && c->bulklen != -1
        && c->bulklen >= PROTO_MBULK_BIG_ARG)
    {
        ssize_t remaining = (size_t)(c->bulklen+2)-sdslen(c->querybuf);

        if (remaining < readlen) readlen = remaining;
    }

    qblen = sdslen(c->querybuf);
    //if (c->querybuf_peak < qblen) c->querybuf_peak = qblen;
    c->querybuf = sdsMakeRoomFor(c->querybuf, readlen);
    nread = read(fd, c->querybuf+qblen, readlen);
    if (nread == -1) {
        if (errno == EAGAIN) {
            return;
        } else {
            serverLog(LL_VERBOSE, "Reading from client: %s",strerror(errno));
            freeClient(c); 
            return;
        }
    } else if (nread == 0) {
        serverLog(LL_VERBOSE, "Client closed connection");
        freeClient(c); 
        return;
    } 

    sdsIncrLen(c->querybuf,nread);
    //c->lastinteraction = server.unixtime; todo
    // if (c->flags & CLIENT_MASTER) c->read_reploff += nread;
    // server.stat_net_input_bytes += nread;
    // if (sdslen(c->querybuf) > server.client_max_querybuf_len) {
    //     sds ci = catClientInfoString(sdsempty(),c), bytes = sdsempty();

    //     bytes = sdscatrepr(bytes,c->querybuf,64);
    //     serverLog(LL_WARNING,"Closing client that reached max query buffer length: %s (qbuf initial bytes: %s)", ci, bytes);
    //     sdsfree(ci);
    //     sdsfree(bytes);
    //     freeClient(c);
    //     return;
    // }

    /* Time to process the buffer. If the client is a master we need to
     * compute the difference between the applied offset before and after
     * processing the buffer, to understand how much of the replication stream
     * was actually applied to the master state: this quantity, and its
     * corresponding part of the replication stream, will be propagated to
     * the sub-slaves and to the replication backlog. */
    //if (!(c->flags & CLIENT_MASTER)) {
        processInputBuffer(c);
    // } else {
    //     size_t prev_offset = c->reploff;
    //     processInputBuffer(c);
    //     size_t applied = c->reploff - prev_offset;
    //     if (applied) {
    //         replicationFeedSlavesFromMasterStream(server.slaves,
    //                 c->pending_querybuf, applied);
    //         sdsrange(c->pending_querybuf,applied,-1);
    //     }
    //}
}

client *createClient(int fd) {
    client *c = zmalloc(sizeof(client));

    /* passing -1 as fd it is possible to create a non connected client.
     * This is useful since all the commands needs to be executed
     * in the context of a client. When commands are executed in other
     * contexts (for instance a Lua script) we need a non connected client. */
    if (fd != -1) {
        anetNonBlock(NULL,fd);
        //anetEnableTcpNoDelay(NULL,fd); todo
        // if (server.tcpkeepalive)
        //     anetKeepAlive(NULL,fd,server.tcpkeepalive);
        if (aeCreateFileEvent(server.el,fd,AE_READABLE,
            readQueryFromClient, c) == AE_ERR)
        {
            close(fd);
            zfree(c);
            return NULL;
        }
    }

    selectDb(c,0);
    uint64_t client_id;

    client_id = server.next_client_id;
    server.next_client_id += 1;

    c->id = client_id;
    c->fd = fd;
    c->name = NULL;
    c->bufpos = 0;
    c->querybuf = sdsempty();
    c->reqtype = 0;
    c->argc = 0;
    c->argv = NULL;
    c->cmd = c->lastcmd = NULL;
    c->multibulklen = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->flags = 0;
    c->pubsub_channels = dictCreate(&objectKeyPointerValueDictType,NULL);
    //c->ctime = c->lastinteraction = server.unixtime; todo.1
    c->reply = listCreate();
    c->reply_bytes = 0;
    listSetFreeMethod(c->reply,freeClientReplyValue);
    listSetDupMethod(c->reply,dupClientReplyValue);
    if (fd != -1) linkClient(c);
    return c;
}

static void acceptCommonHandler(int fd, int flags, char *ip) {
    client *c;
    if ((c = createClient(fd)) == NULL) {
        serverLog(LL_WARNING,
            "Error registering fd event for the new client: %s (fd=%d)",
            strerror(errno),fd);
        close(fd); /* May be already closed, just ignore errors */
        return;
    }
    /* If maxclient directive is set and this is one client more... close the
     * connection. Note that we create the client instead to check before
     * for this condition, since now the socket is already set in non-blocking
     * mode and we can send an error for free using the Kernel I/O */
    if (listLength(server.clients) > server.maxclients) {
        char *err = "-ERR max number of clients reached\r\n";

        /* That's a best effort error message, don't check write errors */
        if (write(c->fd,err,strlen(err)) == -1) {
            /* Nothing to do, Just to avoid the warning... */
        }
        //server.stat_rejected_conn++; todo
        freeClient(c); 
        return;
    }

    /* If the server is running in protected mode (the default) and there
     * is no password set, nor a specific interface is bound, we don't accept
     * requests from non loopback interfaces. Instead we try to explain the
     * user what to do to fix it if needed. */
    if (//server.protected_mode &&
        0 &&
        server.bindaddr_count == 0 &&
        //server.requirepass == NULL &&
        //!(flags & CLIENT_UNIX_SOCKET) &&
        ip != NULL)
    {
        if (strcmp(ip,"127.0.0.1") && strcmp(ip,"::1")) {
            char *err =
                "-DENIED Redis is running in protected mode because protected "
                "mode is enabled, no bind address was specified, no "
                "authentication password is requested to clients. In this mode "
                "connections are only accepted from the loopback interface. "
                "If you want to connect from external computers to Redis you "
                "may adopt one of the following solutions: "
                "1) Just disable protected mode sending the command "
                "'CONFIG SET protected-mode no' from the loopback interface "
                "by connecting to Redis from the same host the server is "
                "running, however MAKE SURE Redis is not publicly accessible "
                "from internet if you do so. Use CONFIG REWRITE to make this "
                "change permanent. "
                "2) Alternatively you can just disable the protected mode by "
                "editing the Redis configuration file, and setting the protected "
                "mode option to 'no', and then restarting the server. "
                "3) If you started the server manually just for testing, restart "
                "it with the '--protected-mode no' option. "
                "4) Setup a bind address or an authentication password. "
                "NOTE: You only need to do one of the above things in order for "
                "the server to start accepting connections from the outside.\r\n";
            if (write(c->fd,err,strlen(err)) == -1) {
                /* Nothing to do, Just to avoid the warning... */
            }
            //server.stat_rejected_conn++; todo
            freeClient(c); 
            return;
        }
    }

    //server.stat_numconnections++; todo
    c->flags |= flags;
}

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd, max = MAX_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];

    while(max--) {
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", server.neterr);
            return;
        }
        serverLog(LL_VERBOSE,"Accepted %s:%d", cip, cport);
        acceptCommonHandler(cfd,0,cip); 
    }
}