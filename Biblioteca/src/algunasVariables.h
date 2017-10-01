#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <signal.h>
#include <netdb.h>
#include <commons/config.h>
#include <commons/string.h>
#include <commons/log.h>
#include <commons/collections/list.h>



#ifndef FUNCIONESGENERICAS_H_
#define FUNCIONESGENERICAS_H_

//PROTOCOLO

#define BACKLOG 10

#define ES_FS 1
#define ES_WORKER 2
#define ES_MASTER 3
#define ES_YAMA 4
#define ES_DATANODE 1000
#define SK_FILE_SEND 300


typedef struct{
	int tipoMsj;
	void* mensaje;
}paquete;


#endif /* FUNCIONESGENERICAS_H_ */
