/*
 * Persistencia.h
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "Estructuras.h"
#include "BitarrayConfiguraciones.h"

#ifndef PERSISTENCIA_H_
#define PERSISTENCIA_H_

void persistirTablaNodo();
void persistirRegistroArchivo();
void persistirTablaArchivo(tablaArchivos* );
void persistirBitmap(tablaBitmapXNodos* );
void persistirDirectorio();


#endif /* PERSISTENCIA_H_ */
