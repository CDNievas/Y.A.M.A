
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
#define ESC_CORRECTA 102
#define ESC_INCORRECTA 103
#define REC_LEER 104
#define REC_ESCRIBIR 105

// ARCHIVO DE CONFIGURACION
char* IP_FILESYSTEM;
u_int32_t PUERTO_FILESYSTEM;
char* NOMBRE_NODO;
u_int32_t PUERTO_DATANODE;
char* RUTA_DATABIN;

t_log * loggerDatanode; // Logger
u_int32_t sizeDataBin; // Guarda el tamaño del Databin
u_int32_t cantBloques; // Guarda cantidad de bloques
void * mapArchivo; // Memoria del mmap
struct stat infoDatabin; // Guarda informacion del archivo
int corte; // Corta el while

void cargarDataNode(t_config*);
void realizarHandshakeFS(u_int32_t);
void cargarBin();
int escribirBloque(uint32_t, char *, uint32_t);
void * leerBloque(uint32_t, uint32_t);
void enviarInfoNodo(u_int32_t);
char * recvDeBloque(u_int32_t);

void gen_random(char *, const u_int32_t);

#endif /* FUNCIONESDATANODE_H_ */
