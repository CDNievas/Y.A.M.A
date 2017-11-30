/*
 * Bitarray.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "Estructuras.h"
#include "BitarrayConfiguraciones.h"
#include "../../Biblioteca/src/genericas.c"
#include "../../Biblioteca/src/Socket.h"

void liberarCopiasXBloque(copiasXBloque* copiaBloque){
	free(copiaBloque->bloque);
	free(copiaBloque->copia1->nodo);
	free(copiaBloque->copia2->nodo);
}

void liberarArchivo(tablaArchivos* entradaArchivo){
	list_destroy_and_destroy_elements(entradaArchivo->bloques,(void*)liberarCopiasXBloque);
	free(entradaArchivo->nombreArchivo);
	free(entradaArchivo->tipo);
}

void liberarListaNodos(char* nodo){
	free(nodo);
}

void liberarContenidoNodo(contenidoNodo* contenidoDelNodo){
	free(contenidoDelNodo->nodo);
}

void liberarBitmaps(tablaBitmapXNodos* entradaBitmap){
	bitarray_destroy(entradaBitmap->bitarray);
	free(entradaBitmap->nodo);
}

void liberarDirectorios(t_directory* entradaDirectorio){
	free(entradaDirectorio->nombre);
}

void liberarConexionNodo(datosConexionNodo* entradaConexionNodo){
	free(entradaConexionNodo->ip);
	free(entradaConexionNodo->nodo);
}

void liberarRegistroArchivos(char* nombre){
	free(nombre);
}

void killMe(int signal){
	list_destroy_and_destroy_elements(tablaGlobalArchivos, (void*)liberarArchivo);
	list_destroy_and_destroy_elements(tablaGlobalNodos->nodo,(void*)liberarListaNodos);
	list_destroy_and_destroy_elements(tablaGlobalNodos->contenidoXNodo,(void*)liberarContenidoNodo);
	list_destroy_and_destroy_elements(listaBitmap,(void*)liberarBitmaps);
	list_destroy_and_destroy_elements(listaDirectorios,(void*)liberarDirectorios);
	list_destroy_and_destroy_elements(listaConexionNodos,(void*)liberarConexionNodo);
	list_destroy_and_destroy_elements(registroArchivos,(void*)liberarRegistroArchivos);
}


void pedirBloque(int socketNodo, uint32_t nroBloque, uint32_t cantBytes){

	 // PRUEBA LECTURA
	int tamanioMsg = sizeof(uint32_t) + sizeof(uint32_t);
	void * msg = miMalloc(tamanioMsg,loggerFileSystem,"Fallo en pedirBloque()");

	memcpy(msg+sizeof(uint32_t), &nroBloque, sizeof(uint32_t));
	memcpy(msg+sizeof(uint32_t)+sizeof(uint32_t), &cantBytes, sizeof(uint32_t));

	sendRemasterizado(socketNodo,ENV_LEER,tamanioMsg,msg);

	free(msg);

}

void recibirBloque(int socketNodo){

	uint32_t cantBytes = recibirUInt(socketNodo);
	void * bloque = miMalloc(cantBytes,loggerFileSystem,"Fallo en recibirBloque()");
	if(recv(socket, bloque, cantBytes, MSG_WAITALL) == -1){
		perror("Error al recibir el bloque.");
		exit(-1);
	}
	char* bloqueRecibido = string_substring_until(bloque, cantBytes);

	printf("%s",bloqueRecibido);

	free(bloque);

}

char * obtenerPathBitmap(char * nombreNodo){
	char * path = string_new();
	string_append(&path, PATH_BITMAPS);
	//string_append(&path, "/home/utnso/workspace/tp-2017-2c-ElTPEstaBien/FileSystem/Debug/yamafs:/metadata/bitmap/");
	string_append(&path, nombreNodo);
	string_append(&path, ".dat");
	return path;
}

char * obtenerPathTablaNodo(){
	char * path = string_new();
	string_append(&path, PATH_METADATA);
	//string_append(&path, "/home/utnso/workspace/tp-2017-2c-ElTPEstaBien/FileSystem/Debug/yamafs:/metadata/");
	string_append(&path, "nodos.bin");
	return path;
}

t_bitarray * abrirBitmap(char * nombreNodo,int cantBloques){

	char * path = obtenerPathBitmap(nombreNodo); // Path bitmap
	int archivo= open(path, O_RDWR); // FD Archivo
	struct stat infoBitmap; // Guarda informacion del archivo
	char * mapArchivo; // Memoria del mmap
	t_bitarray * bitarray; // Bitarray

	if(archivo  < 0){
		// Error al abrir el archivo
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo.");
		exit(-1);
	}

	// Abro el archivo
	if(fstat(archivo,&infoBitmap) < 0){
		// Error al abrir el archivo
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo.");
		exit(-1);
	}



	// Lo mapeo a memoria
	mapArchivo = mmap(0, infoBitmap.st_size, PROT_WRITE | PROT_READ, MAP_SHARED, archivo, 0);
	if (mapArchivo == MAP_FAILED) {
				printf("Error al mapear a memoria: %s\n", strerror(errno));
				close(archivo);
	}

	int tamanioBitarray=cantBloques/2;
	 if(cantBloques % 2 != 0){
	  tamanioBitarray++;
	 }

	//int tamanioBitarray=cantBloques;

	bitarray = bitarray_create_with_mode(mapArchivo,tamanioBitarray,MSB_FIRST);

	if(close(archivo) < 0){
		log_error(loggerFileSystem,"Fallo al cerrar el archivo.");
		exit(-1);
	}
	free(path);
	return bitarray;

}

t_bitarray * crearBitmap(char * nombreNodo, int cantBloques){



	log_debug(loggerFileSystem,"Se procede a crear el archivo Bitmap.bin");
	char * path = obtenerPathBitmap(nombreNodo); // Path bitmap
	int file = open(path, O_RDWR | O_CREAT,S_IRUSR | S_IWUSR);
	char* contenido=string_repeat('\0',cantBloques);
	if(file<0){
		log_info(loggerFileSystem,"No se pudo crear el archivo");
	}
	write(file,contenido,cantBloques);
	free(contenido);
	free(path);
	return abrirBitmap(nombreNodo,cantBloques);


}

//Habria que implementar la copia de los archivos

void cargarFileSystem(t_config* configuracionFS) {
	if (!config_has_property(configuracionFS, "PUERTO_ESCUCHA")) {
		log_error(loggerFileSystem,
				"No se encuentra el parametro PUERTO_ESCUCHA en el archivo de configuracion");
		exit(-1);
	}
	PUERTO_ESCUCHA = config_get_int_value(configuracionFS,"PUERTO_ESCUCHA");


	if(!config_has_property(configuracionFS, "PATH_PADRE")){
		log_error(loggerFileSystem, "No se encuentra el parametro PATH_PADRE en el archivo de configuracion");
		exit(-1);
	}

	PATH_PADRE = string_new();
	string_append(&PATH_PADRE, config_get_string_value(configuracionFS, "PATH_PADRE"));



	if (!config_has_property(configuracionFS, "PATH_METADATA")) {
		log_error(loggerFileSystem,"No se encuentra el parametro PATH_METADATA en el archivo de configuracion");
		exit(-1);
	}

	PATH_METADATA = string_new();
	string_append(&PATH_METADATA, config_get_string_value(configuracionFS, "PATH_METADATA"));




	if (!config_has_property(configuracionFS, "PATH_ARCHIVOS")) {
		log_error(loggerFileSystem,"No se encuentra el parametro PATH_ARCHIVOS en el archivo de configuracion");
		exit(-1);
	}
	PATH_ARCHIVOS = string_new();
	string_append(&PATH_ARCHIVOS, config_get_string_value(configuracionFS, "PATH_ARCHIVOS"));



	if (!config_has_property(configuracionFS, "PATH_BITMAPS")) {
		log_error(loggerFileSystem,"No se encuentra el parametro PATH_BITMAPS en el archivo de configuracion");
		exit(-1);
	}
	PATH_BITMAPS = string_new();
	string_append(&PATH_BITMAPS,  config_get_string_value(configuracionFS, "PATH_BITMAPS"));


	config_destroy(configuracionFS);

}
