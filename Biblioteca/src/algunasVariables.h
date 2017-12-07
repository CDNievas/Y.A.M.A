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
#include <commons/collections/queue.h>
#include <commons/collections/dictionary.h>
#include <math.h>
#include <semaphore.h>


#ifndef FUNCIONESGENERICAS_H_
#define FUNCIONESGENERICAS_H_

// COLORES PARA IMPRIMIR

#define KNRM  "\x1B[0m"
#define KRED  "\x1B[31m"
#define KGRN  "\x1B[32m"
#define KYEL  "\x1B[33m"
#define KBLU  "\x1B[34m"
#define KMAG  "\x1B[35m"
#define KCYN  "\x1B[36m"
#define KWHT  "\x1B[37m"
#define RESET "\033[0m"

//PROTOCOLO

#define BACKLOG 5

#define ES_FS 5001
#define ES_WORKER 5002
#define ES_MASTER 5003
#define ES_YAMA 5004
#define ES_OTRO_WORKER 5030
#define ES_DATANODE 6000
#define SK_FILE_SEND 300

typedef struct{
	int tipoMsj;
	void* mensaje;
}paquete;


#endif /* FUNCIONESGENERICAS_H_ */
