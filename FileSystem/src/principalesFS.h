/*
 * principalesFS.h
 *
 *  Created on: 3/12/2017
 *      Author: utnso
 */

#ifndef PRINCIPALESFS_H_
#define PRINCIPALESFS_H_


#include "estructuras.h"

void inicializarDirectoriosPrincipales();
//Funciones de bitmap
t_bitarray * crearBitmap(int);

// Funciones principales
void handlerSIGINT();
void chequearParametrosFS(int, char *);
void limpiarFS();
void cargarConfigFS(t_config *);
void iniciarEstructuras();
void liberarMemoria();
void liberarTablaArchivos();
void liberarBitmaps();
void liberarTablaDirectorios();
void liberarTablaNodos();

// Funciones de sockets
int iniciarServidor(int);
void atenderConexion();
void atenderNotificacion();
void verificarSiNodo();
void meterSocket(int);
void sacarSocket(int);

#endif /* PRINCIPALESFS_H_ */
