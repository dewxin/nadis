
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <netdb.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>

#include "anet.h"

static void anetSetError(char *err, const char *fmt, ...)
{
    va_list ap;

    if (!err) return;
    va_start(ap, fmt);
    vsnprintf(err, ANET_ERR_LEN, fmt, ap);
    va_end(ap);
}


static int anetSetReuseAddr(char *err, int fd) {
    int yes = 1;
    /* Make sure connection-intensive things like the redis benckmark
     * will be able to close/open sockets a zillion of times */
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
        anetSetError(err, "setsockopt SO_REUSEADDR: %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

static int anetListen(char *err, int socketId, struct sockaddr *socketAddress, socklen_t addrLen, int backlog) {
    if (bind(socketId,socketAddress,addrLen) == -1) {
        anetSetError(err, "bind: %s", strerror(errno));
        close(socketId);
        return ANET_ERR;
    }

    if (listen(socketId, backlog) == -1) {
        anetSetError(err, "listen: %s", strerror(errno));
        close(socketId);
        return ANET_ERR;
    }
    return ANET_OK;
}

static int anetV6Only(char *err, int s) {
    int yes = 1;
    if (setsockopt(s,IPPROTO_IPV6,IPV6_V6ONLY,&yes,sizeof(yes)) == -1) {
        anetSetError(err, "setsockopt: %s", strerror(errno));
        close(s);
        return ANET_ERR;
    }
    return ANET_OK;
}

int anetSetBlock(char *err, int fd, int non_block) {
    int flags;

    /* Set the socket blocking (if non_block is zero) or non-blocking.
     * Note that fcntl(2) for F_GETFL and F_SETFL can't be
     * interrupted by a signal. */
    if ((flags = fcntl(fd, F_GETFL)) == -1) {
        anetSetError(err, "fcntl(F_GETFL): %s", strerror(errno));
        return ANET_ERR;
    }

    if (non_block)
        flags |= O_NONBLOCK;
    else
        flags &= ~O_NONBLOCK;

    if (fcntl(fd, F_SETFL, flags) == -1) {
        anetSetError(err, "fcntl(F_SETFL,O_NONBLOCK): %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

int anetNonBlock(char *err, int fd) {
    return anetSetBlock(err,fd,1);
}

static int _anetTcpServer(char *err, int port, char *bindaddr, int addressFamily, int backlog)
{
    int socketId = -1, errCode;
    char _port[6];  /* strlen("65535") */
    struct addrinfo hints, *servinfo, *p;

    snprintf(_port,6,"%d",port);
    memset(&hints,0,sizeof(hints));
    hints.ai_family = addressFamily;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;    /* No effect if bindaddr != NULL */

    if ((errCode = getaddrinfo(bindaddr,_port,&hints,&servinfo)) != 0) {
        anetSetError(err, "%s", gai_strerror(errCode));
        return ANET_ERR;
    }

    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((socketId = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
            continue;

        if (addressFamily == AF_INET6 && anetV6Only(err,socketId) == ANET_ERR) goto error;
        if (anetSetReuseAddr(err, socketId) == ANET_ERR) goto error;
        if (anetListen(err,socketId,p->ai_addr,p->ai_addrlen,backlog) == ANET_ERR) socketId = ANET_ERR;
        goto end;
    }
    if (p == NULL) {
        anetSetError(err, "unable to bind socket, errno: %d", errno);
        goto error;
    }

error:
    if (socketId != -1) close(socketId);
    socketId = ANET_ERR;
end:
    freeaddrinfo(servinfo);
    return socketId;
}

int anetTcpServer(char *err, int port, char *bindaddr, int backlog)
{
    return _anetTcpServer(err, port, bindaddr, AF_INET, backlog);
}

int anetTcp6Server(char *err, int port, char *bindaddr, int backlog)
{
    return _anetTcpServer(err, port, bindaddr, AF_INET6, backlog);
}

/**
 * @brief if it is interrupted by the system call , then try again
 *          else return ANET_ERR or fd
 * 
 * @param err 
 * @param fd 
 * @param sa 
 * @param len 
 * @return int 
 */
static int anetGenericAccept(char *err, int fd, struct sockaddr *sa, socklen_t *len) {
    int retfd;
    while(1) {
        /*
        If no pending connections are present on the queue, and the socket is
       not marked as nonblocking, accept() blocks the caller until a
       connection is present.  If the socket is marked nonblocking and no
       pending connections are present on the queue, accept() fails with the
       error EAGAIN or EWOULDBLOCK.
        */
        retfd = accept(fd,sa,len);
        if (retfd == -1) {
            if (errno == EINTR)
                continue;
            else {
                anetSetError(err, "accept: %s", strerror(errno));
                return ANET_ERR;
            }
        }
        break;
    }
    return retfd;
}


int anetTcpAccept(char *err, int s, char *ip, size_t ip_len, int *port) {
    int fd;
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);
    if ((fd = anetGenericAccept(err,s,(struct sockaddr*)&sa,&salen)) == -1)
        return ANET_ERR;

    if (sa.ss_family == AF_INET) {
        struct sockaddr_in *s = (struct sockaddr_in *)&sa;
        if (ip) inet_ntop(AF_INET,(void*)&(s->sin_addr),ip,ip_len);
        if (port) *port = ntohs(s->sin_port);
    } else {
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&sa;
        if (ip) inet_ntop(AF_INET6,(void*)&(s->sin6_addr),ip,ip_len);
        if (port) *port = ntohs(s->sin6_port);
    }
    return fd;
}