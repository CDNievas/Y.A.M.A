/*
 * principalesFS.h
 *
 *  Created on: 3/12/2017
 *      Author: utnso
 */

#ifndef PRINCIPALESFS_H_
#define PRINCIPALESFS_H_


#include "estructuras.h"
#include "persistencia.h"
#include "funcionesYama.h"
#include "funcionesWorker.h"
#include "nodos.h"

//Funciones de bitmap
t_bitarray * crearBitmap(int);

// Funciones principales
void handlerSIGINT();
void chequearParametrosFS(int, char *);
void limpiarFS();
void cargarConfigFS(t_config *);
void iniciarEstructuras();
void destuirMetadata();
void inicializarDirectoriosPrincipales();
void iniciarTablaDeDirectorios();


// Funciones auxiliares
char * obtenerPathTablaNodo();
char * obtenerPathDirectorio();
char * obtenerPathArchivo(uint32_t);
char * obtenerPathBitmap(char *);

// Libera memoria
void liberarMemoria();
void liberarTablaArchivos();
void liberarBitmaps();
void liberarTablaDirectorios();
void liberarTablaNodos();
void limpiarEstructurasAdministrativas();
void liberarListaRegistroArchivos();
void liberarlistaConexionNodos();

// Funciones de sockets
int iniciarServidor(int);
void atenderConexion();
void atenderNotificacion();
void verificarSiNodo();

#endif /* PRINCIPALESFS_H_ */
