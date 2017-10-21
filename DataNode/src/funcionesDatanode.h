
#ifndef FUNCIONESDATANODE_H_
#define FUNCIONESDATANODE_H_

#include "../../Biblioteca/src/configParser.h"
#include "../../Biblioteca/src/Socket.h"
#include <sys/stat.h>
#include <commons/bitarray.h>
#include <sys/mman.h>

#define LOGFILE "ggwp.log"
#define SIZEBLOQUE 1048576

// ARCHIVO DE CONFIGURACION
int PUERTO_FILESYSTEM;
char* IP_FILESYSTEM;
char* RUTA_DATABIN;
char* NOMBRE_NODO;
int PUERTO_DATANODE;

int codError; // Variable que se usa para absorber el codigo de error de una funcion
t_log * loggerDatanode; // Logger
int sizeDataBin; // Guarda el tamaño del Databin
char * pathDataBin; // Guarda el path del databin
t_bitarray * bitarray; // Bitarray del .bin
void * bmap; // Memoria del mmap
struct stat infoDatabin; // Guarda informacion del archivo

void cargarDataNode(t_config*);
void realizarHandshakeFS(int);
t_bitarray * cargarBin(void *);

#endif /* FUNCIONESDATANODE_H_ */
