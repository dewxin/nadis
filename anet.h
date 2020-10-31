#ifndef ANET_H
#define ANET_H


#define ANET_OK 0
#define ANET_ERR -1
#define ANET_ERR_LEN 256



int anetTcpServer(char *err, int port, char *bindaddr, int backlog);
int anetTcp6Server(char *err, int port, char *bindaddr, int backlog);

int anetSetBlock(char *err, int fd, int non_block);
int anetNonBlock(char *err, int fd);

int anetTcpAccept(char *err, int s, char *ip, size_t ip_len, int *port);
#endif