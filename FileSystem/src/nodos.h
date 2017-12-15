/*
 * nodos.h
 *
 *  Created on: 4/12/2017
 *      Author: utnso
 */

#ifndef NODOS_H_
#define NODOS_H_

#include "estructuras.h"

void actualizoBitmapsNodosDisponibles();
void limpiarNodosDesonectados();
void verificarCopiasNodo(char* );
int cantidadDeNodosDisponibles();
void perteneceAlSistema(char* , int , char* , uint32_t );
bool hayUnEstadoEstable();
void registrarNodosConectados();
void verificarSiEsNodoDesconectado(char* , uint32_t ,char* ,uint32_t );
int asignarBloqueNodo(strNodo* );
void registrarNodo(int);
void enviarDatosANodo(t_list* ,FILE* ,strArchivo* );
bool asignarEnviarANodo(void* , uint32_t , strBloqueArchivo* );
void mostrarEstadoDelSistemaNodos();

#endif /* NODOS_H_ */
