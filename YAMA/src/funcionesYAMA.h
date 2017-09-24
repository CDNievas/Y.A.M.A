/*
 * funcionesYAMA.h
 *
 *  Created on: 22/9/2017
 *      Author: utnso
 */
#include "estructuras.h"

#ifndef FUNCIONESYAMA_H_
#define FUNCIONESYAMA_H_

administracionYAMA* generarAdministracion();
conexionNodo* generarConexionNodo();
char* obtenerNombreTemporalLocal();
char* obtenerNombreTemporalGlobal();
char* obtenerNombreTemporalTransformacion();
int obtenerNumeroDeJob();
int obtenerNumeroDeMaster();
void obtenerIPYPuerto(conexionNodo*);
void liberarConexion(conexionNodo*);
void liberarInfoFS(infoDeFs*);
t_list* obtenerListaDelNodo(int, void*);
char* obtenerNombreNodo(t_list*);
int obtenerJobDeNodo(t_list*);
void cargarYAMA(t_config*);
void realizarHandshakeConFS(int);
void chequeameLaSignal(int);

#endif /* FUNCIONESYAMA_H_ */
