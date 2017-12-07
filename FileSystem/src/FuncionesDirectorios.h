/*
 * FuncionesDirectorios.h
 *
 *  Created on: 2/12/2017
 *      Author: utnso
 */

#ifndef FUNCIONESDIRECTORIOS_H_
#define FUNCIONESDIRECTORIOS_H_

#include "Estructuras.h"

int cantParamCom(char **);
bool chequearParamCom(char **, int,int);
bool contieneYamafs(char *);
bool existePath(char *);
bool recorrerPath(char **,int ,int );
tablaArchivos* existeArchivoPath(char * ,int );
t_directory* existeDirectorioPath(char * , int );
char * obtenerArchivo(char *,int);

/*
int buscarYamafs(char **);
tablaArchivos* esArchivoPath(char *,int );
t_directory* esDirectorioPath(char *, int );
bool recorrerPath(char **,int ,int );
bool existePath(char * pathDirectorio);
void borrarArchivo(char * path);
void borrarDirectorio(char * path);
int obtenerIdPadreArchivo(char ** pathDesc,int indice,int idPadre);
void obtenerIdPadreDirectorio(char ** ,int ,int , int * , int * );
void renamePath(char *, char *);
*/

#endif /* FUNCIONESDIRECTORIOS_H_ */

