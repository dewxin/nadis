params= -O0 -g3 -D HAVE_EPOLL
nadis: server.o networking.o dict.o sds.o adlist.o anet.o ae.o zmalloc.o util.o object.o sha1.o t_string.o siphash.o db.o pubsub.o debug.o expire.o notify.o
	cc $(params) -o nadis  *.o

notify.o: notify.c server.h
	cc $(params) -c notify.c

debug.o: debug.c server.h
	cc $(params) -c debug.c

expire.o: expire.c server.h
	cc $(params) -c expire.c

pubsub.o: pubsub.c server.h
	cc $(params) -c pubsub.c

sha1.o: sha1.c sha1.h
	cc $(params) -c sha1.c

object.o: object.c server.h
	cc $(params) -c object.c

util.o: util.c util.h
	cc $(params) -c util.c

server.o: server.c server.h 
	cc $(params) -c  server.c

networking.o: networking.c server.h
	cc $(params) -c networking.c

dict.o: dict.c dict.h
	cc $(params) -c  dict.c

sds.o: sds.c sds.h
	cc $(params) -c sds.c

adlist.o: adlist.c adlist.h
	cc $(params) -c adlist.c

anet.o: anet.c anet.h
	cc $(params) -c  anet.c

ae.o: ae.c ae.h
	cc $(params) -c ae.c

zmalloc.o: zmalloc.c zmalloc.h
	cc $(params) -c zmalloc.c

t_string.o: t_string.c server.h
	cc $(params) -c t_string.c

siphash.o: siphash.c
	cc $(params) -c siphash.c

db.o: db.c server.h
	cc $(params) -c db.c


clean: 
	rm *.o
