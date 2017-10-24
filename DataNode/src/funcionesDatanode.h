
#ifndef FUNCIONESDATANODE_H_
#define FUNCIONESDATANODE_H_

#include "../../Biblioteca/src/genericas.h"
#include "../../Biblioteca/src/configParser.h"
#include "../../Biblioteca/src/Socket.h"
#include <sys/stat.h>
#include <commons/bitarray.h>
#include <sys/mman.h>

// CONFIGS
#define LOGFILE "ggwp.log"
#define SIZEBLOQUE 1048576

// PROTOCOLO
#define ENV_INFONODO 100
#define ENV_BLOQUE 101
#define ENV_CONFESCRITURA 102
#define REC_LEER 1
#define REC_ESCRIBIR 2
#define BLOQUEINEXISTENTE 3

// ARCHIVO DE CONFIGURACION
u_int32_t puertoFilesystem;
char* ipFilesystem;
char* rutaDatabin;
char* nombreNodo;
u_int32_t puertoDatanode;

t_log * loggerDatanode; // Logger
u_int32_t sizeDataBin; // Guarda el tamaño del Databin
u_int32_t cantBloques; // Guarda cantidad de bloques
void * mapArchivo; // Memoria del mmap
struct stat infoDatabin; // Guarda informacion del archivo
int corte; // Corta el while

void cargarDataNode(t_config*);
void realizarHandshakeFS(u_int32_t);
void cargarBin();
int escribirBloque(u_int32_t, void *);
void * leerBloque(u_int32_t);
void enviarInfoNodo(u_int32_t);
void * recvDeBloque(u_int32_t);

void gen_random(char *, const u_int32_t);

#endif /* FUNCIONESDATANODE_H_ */
