/*
 * EstadoAnterior.h
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */
#include "Estructuras.h"

#ifndef ESTADOANTERIOR_H_
#define ESTADOANTERIOR_H_

void cargarEstructuraBitmap();
void cargarEstructuraDirectorio(t_config* );
void cargarEstructuraNodos(t_config* );

void cargarTablaArchivo(char* );

void cargarEstructuraArchivos(t_config* );

bool hayUnEstadoAnterior();

#endif /* ESTADOANTERIOR_H_ */
