/*
 * configParser.h
 *
 *  Created on: 15/9/2017
 *      Author: utnso
 */

#include "algunasVariables.h"

#ifndef SRC_CONFIGPARSER_H_
#define SRC_CONFIGPARSER_H_

void chequearParametros(int, int);
void chequearExistenciaArchivo(char*);
void chequearCantidadDeKeys(t_config*, int);
t_config* generarTConfig(char*, int);


#endif /* SRC_CONFIGPARSER_H_ */
