#include "structMaster.h"

#ifndef FUNCIONESMASTER_H_
#define FUNCIONESMASTER_H_

void cargarMaster(t_config*);
void realizarHandshake(int, int);
long int obtenerTamanioArchivo(FILE*);
char* leerArchivo(FILE*, long int);
char* obtenerContenido(char*);
void propagarArchivo(char*, int);
void verificarNodo(char*);
uint32_t tamanioArchivoAModificar();
void* serializarArchivoAModificar();


#endif /* FUNCIONESMASTER_H_ */