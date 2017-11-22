/*
 * transformacion.h
 *
 *  Created on: 22/9/2017
 *      Author: utnso
 */
#include "estructuras.h"
#include "funcionesYAMA.h"
#include "serializaciones.h"
#include "balanceoDeCargas.h"


#ifndef TRANSFORMACION_H_
#define TRANSFORMACION_H_

void solicitarArchivo(char*);
t_list *recibirInfoArchivo();
char* recibirNombreArchivo(int);
//CARGO LA TRANSFORMACION EN LA TABLA, OBTENGO LOS NODOS A USAR.
int cargarTransformacion(int, int, t_list*, t_list*);
void terminarTransformacion(int, int, char*);

bool hayQueReplanificar(administracionYAMA*, t_list*);
t_list* filtrarTablaFallida(uint32_t, char*);
//Actualizo la tabla de estados con el nodo fallido
void cargarFallo(uint32_t, char*);
t_list* obtenerBloquesFallidos(uint32_t, char*);
//Replanifico con la otra copia y cargo en la tabla de estados
bool cargarReplanificacion(int, uint32_t, char*, t_list*);

#endif /* TRANSFORMACION_H_ */
