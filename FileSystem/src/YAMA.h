/*
 * YAMA.h
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "Estructuras.h"

#ifndef YAMA_H_
#define YAMA_H_

int sacarTamanioMensaje() ;

void enviarListaNodos(int ) ;

int tamanioStruct(copiasXBloque* );

void* serializarCopiaBloque(copiasXBloque* );

int sacarTamanio(tablaArchivos* );


void enviarTablaAYama(int , tablaArchivos* );


void enviarDatoArchivo(int );

void enviarDatosConexionNodo(int );

#endif /* YAMA_H_ */
