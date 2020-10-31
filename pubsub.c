#include "server.h"

int clientSubscriptionsCount(client *c) {
    return dictSize(c->pubsub_channels);//+
           //listLength(c->pubsub_patterns); todo
}

/* Subscribe a client to a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was already subscribed to that channel. */
int pubsubSubscribeChannel(client *c, robj *channel) {
    dictEntry *de;
    list *clients = NULL;
    int retval = 0;

    /* Add the channel to the client -> channels hash table */
    if (dictAdd(c->pubsub_channels,channel,NULL) == DICT_OK) {
        retval = 1;
        incrRefCount(channel);
        /* Add the client to the channel -> list of clients hash table */
        de = dictFind(server.pubsub_channels,channel);
        if (de == NULL) {
            clients = listCreate();
            dictAdd(server.pubsub_channels,channel,clients);
            incrRefCount(channel);
        } else {
            clients = dictGetVal(de);
        }
        listAddNodeTail(clients,c);
    }
    /* Notify the client */
    addReply(c,shared.mbulkhdr[3]);
    addReply(c,shared.subscribebulk);
    addReplyBulk(c,channel);
    addReplyLongLong(c,clientSubscriptionsCount(c));
    return retval;
}

/* Publish a message */
int pubsubPublishMessage(robj *channel, robj *message) {
    int receivers = 0;
    dictEntry *de;
    // listNode *ln;
    // listIter li;

    /* Send to clients listening for that channel */
    de = dictFind(server.pubsub_channels, channel);
    if (de) {
        list *list = dictGetVal(de);
        listNode *ln;
        listIter li;

        listRewind(list,&li);
        while ((ln = listNext(&li)) != NULL) {
            client *c = ln->value;

            addReply(c,shared.mbulkhdr[3]);
            addReply(c,shared.messagebulk);
            addReplyBulk(c,channel);
            addReplyBulk(c,message);
            receivers++;
        }
    }
    /* Send to clients listening to matching channels */
    // if (listLength(server.pubsub_patterns)) {
    //     listRewind(server.pubsub_patterns,&li);
    //     channel = getDecodedObject(channel);
    //     while ((ln = listNext(&li)) != NULL) {
    //         pubsubPattern *pat = ln->value;

    //         if (stringmatchlen((char*)pat->pattern->ptr,
    //                             sdslen(pat->pattern->ptr),
    //                             (char*)channel->ptr,
    //                             sdslen(channel->ptr),0)) {
    //             addReply(pat->client,shared.mbulkhdr[4]);
    //             addReply(pat->client,shared.pmessagebulk);
    //             addReplyBulk(pat->client,pat->pattern);
    //             addReplyBulk(pat->client,channel);
    //             addReplyBulk(pat->client,message);
    //             receivers++;
    //         }
    //     }
    //     decrRefCount(channel);
    // }
    return receivers;
}

void subscribeCommand(client *c) {
    int j;

    for (j = 1; j < c->argc; j++)
        pubsubSubscribeChannel(c, c->argv[j]);
    c->flags |= CLIENT_PUBSUB;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
int pubsubUnsubscribeChannel(client *c, robj *channel, int notify) {
    dictEntry *de;
    list *clients;
    listNode *ln;
    int retval = 0;

    /* Remove the channel from the client -> channels hash table */
    incrRefCount(channel); /* channel may be just a pointer to the same object
                            we have in the hash tables. Protect it... */
    if (dictDelete(c->pubsub_channels,channel) == DICT_OK) {
        retval = 1;
        /* Remove the client from the channel -> clients list hash table */
        de = dictFind(server.pubsub_channels,channel);
        //serverAssertWithInfo(c,NULL,de != NULL); todo
        clients = dictGetVal(de);
        ln = listSearchKey(clients,c);
        //serverAssertWithInfo(c,NULL,ln != NULL);
        listDelNode(clients,ln);
        if (listLength(clients) == 0) {
            /* Free the list and associated hash entry at all if this was
             * the latest client, so that it will be possible to abuse
             * Redis PUBSUB creating millions of channels. */
            dictDelete(server.pubsub_channels,channel);
        }
    }
    /* Notify the client */
    if (notify) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.unsubscribebulk);
        addReplyBulk(c,channel);
        addReplyLongLong(c,dictSize(c->pubsub_channels));//+
                       //listLength(c->pubsub_patterns));

    }
    decrRefCount(channel); /* it is finally safe to release it */
    return retval;
}

/* Unsubscribe from all the channels. Return the number of channels the
 * client was subscribed to. */
int pubsubUnsubscribeAllChannels(client *c, int notify) {
    dictIterator *di = dictGetSafeIterator(c->pubsub_channels);
    dictEntry *de;
    int count = 0;

    while((de = dictNext(di)) != NULL) {
        robj *channel = dictGetKey(de);

        count += pubsubUnsubscribeChannel(c,channel,notify);
    }
    /* We were subscribed to nothing? Still reply to the client. */
    if (notify && count == 0) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.unsubscribebulk);
        addReply(c,shared.nullbulk);
        addReplyLongLong(c,dictSize(c->pubsub_channels));//+
                       //listLength(c->pubsub_patterns));
    }
    dictReleaseIterator(di);
    return count;
}




void unsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllChannels(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribeChannel(c,c->argv[j],1);
    }
    if (clientSubscriptionsCount(c) == 0) c->flags &= ~CLIENT_PUBSUB;
}


void publishCommand(client *c) {
    int receivers = pubsubPublishMessage(c->argv[1],c->argv[2]);
    // if (server.cluster_enabled) todo.REPL
    //     clusterPropagatePublish(c->argv[1],c->argv[2]);
    // else
    //     forceCommandPropagation(c,PROPAGATE_REPL);
    addReplyLongLong(c,receivers);
}