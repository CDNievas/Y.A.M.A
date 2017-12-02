/*
 * FuncionesFS.h
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "Estructuras.h"
#include "Nodos.h"
#include "Persistencia.h"


#ifndef FUNCIONESFS_H_
#define FUNCIONESFS_H_


void killMe(int);

//------------------------FUNCIONES DIRECTORIOS
void inicializarDirectoriosPrincipales();
void liberarComandoDesarmado(char** );
void liberarDirectorio(t_directory* );
char* obtenerNombreDirectorio(char** );
int obtenerDirectorioPadre(char** );
t_directory* createDirectory();
void moveDirectory(char* , char* );
bool existeDirectory(char* );
int crearDirectorio(char* );
void renameDirectory(char* , char* );
int obtenerIndexDirectorio(char* );
int deleteDirectory(char* );

//---------------------ALMACENAR

int sacarTamanioArchivo(FILE* ) ;

int asignarBloqueNodo(contenidoNodo* );

void asignarEnviarANodo(void* ,uint32_t ,copiasXBloque* );

void enviarDatosANodo(t_list* ,FILE* , tablaArchivos* );
bool elSistemaAguantaElArchivo(uint32_t );

bool almacenarArchivo(char* , char* ,char* ) ;

//------------------------------------------------LEER
void leerArchivo(char* ,char* );






#endif /* FUNCIONESFS_H_ */
