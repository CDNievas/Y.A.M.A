/*
 * funcionesConsola.h
 *
 *  Created on: 7/12/2017
 *      Author: utnso
 */

#include "estructuras.h"
#include "persistencia.h"
#include "funcionesPath.h"
#include "principalesFS.h"

#ifndef FUNCIONESCONSOLA_H_
#define FUNCIONESCONSOLA_H_

void liberarRutaDesarmada(char** ruta);

bool buscarArchivo(char * path);
int cantParam(char **);
bool chequearParamCom(char **, int , int);
bool contieneYamafs(char *);
int crearDirectorio(char *);
bool almacenarArchivo(char*, char*, char*);
int borrarDirectorio(char *);
int borrarArchivo(char *);
uint32_t sacarTamanioArchivo (FILE*);
int renombrarPath(char *, char *);
void catArchivo(char *);
void liberarBloque(char* , uint32_t );
void liberarArchivoYPersistir(strArchivo* );
int sacarCantidadbloqueLibre(strNodo* );
void liberarBloque(char*, uint32_t );
void liberarArchivoYPersistir(strArchivo* );
int sacarCantidadBloquesLibres(strNodo* nodo);
void actualizarEstructurasNodos();
void borrarBloquesArchivos(strArchivo* );
void crearDirectorios(char* , char* );

char * obtenerBloque(int, uint32_t);
int funcionCat(strBloqueArchivo *);
void catArchivo(char *);
int catCpto(strBloqueArchivo *,FILE*);
int cpto(char *, char*);
void funcionLs(char* pathDirectorio);



#endif /* FUNCIONESCONSOLA_H_ */
