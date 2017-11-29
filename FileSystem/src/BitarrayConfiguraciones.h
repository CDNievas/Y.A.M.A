/*
 * Bitarray.h
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */
#include "Estructuras.h"


#ifndef BITARRAYCONFIGURACIONES_H_
#define BITARRAYCONFIGURACIONES_H_


void killMe(int);
void pedirBloque(int , uint32_t , uint32_t );
void recibirBloque(int );

char * obtenerPathBitmap(char * );

char * obtenerPathTablaNodo();

t_bitarray * abrirBitmap(char * ,int );

t_bitarray * crearBitmap(char * , int );



void cargarFileSystem(t_config* );




#endif /* BITARRAYCONFIGURACIONES_H_ */
