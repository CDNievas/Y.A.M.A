/*
 * serializaciones.h
 *
 *  Created on: 22/9/2017
 *      Author: utnso
 */

#include "estructuras.h"

#ifndef SERIALIZACIONES_H_
#define SERIALIZACIONES_H_

uint32_t obtenerTamanioCopia(copia*, conexionNodo*);
copia* deserializarCopia(void*);
void* serializarCopia(copia*, conexionNodo*);
void *serializarInfoTransformacion(t_list*);
uint32_t obtenerTamanioInfoTransformacion(t_list*);
void* serializarInfoReduccionLocal(conexionNodo*, char*, t_list*);
uint32_t obtenerTamanioInfoReduccionLocal(conexionNodo*, char*, t_list*);
uint32_t obtenerTamanioReduGlobal(administracionYAMA*, t_list*, t_list*);
/*
 * fila de reduccion global de la tabla de estados
 * Lista con las conexiones de los nodos
 * Lista con los datos de administracion de todos los nodos involucrados
 */
uint32_t obtenerTamanioInfoReduccionGlobal(administracionYAMA*, t_list*, t_list*);
void* serializarInfoReduccionGlobal(administracionYAMA*, t_list*, t_list*); //MISMOS PARAMETROS QUE LA ANTERIOR
//Nombre nodo encargado y lista de filas de la tabla de estados involucradas
administracionYAMA* obtenerAdminNodoEncargado(char*, t_list*);
//Nombre nodo encargado y lista de conexiones de nodos involucrados
conexionNodo* obtenerConexionNodoEncargado(char*, t_list*);

//ENVIO DE DATOS PARA ALMACENAMIENTO FINAL
uint32_t obtenerTamanioInfoAlmacenamientoFinal(conexionNodo*, char*);
void* serializarInfoAlmacenamientoFinal(conexionNodo*, char*);
#endif /* SERIALIZACIONES_H_ */
