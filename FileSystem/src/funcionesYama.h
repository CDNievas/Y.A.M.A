/*
 * funcionesYama.h
 *
 *  Created on: 9/12/2017
 *      Author: utnso
 */

#include "estructuras.h"

#ifndef FUNCIONESYAMA_H_
#define FUNCIONESYAMA_H_

uint32_t sacarTamanioMensaje ();

void enviarListaNodos(int );

int tamanioStruct(strBloqueArchivo* );

void* serializarCopiaBloque(strBloqueArchivo* );

int sacarTamanio(strArchivo* );

void enviarTablaAYama(int , strArchivo* );

void enviarDatoArchivo(int );

void enviarDatosConexionNodo(int );


#endif /* FUNCIONESYAMA_H_ */
