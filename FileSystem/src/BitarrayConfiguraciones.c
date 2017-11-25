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
	string_append(&path, PATH_METADATA);
	string_append(&path, "bitmap/");
	string_append(&path, nombreNodo);
	string_append(&path, ".dat");
	return path;
}

char * obtenerPathTablaNodo(){
	char * path = string_new();
	string_append(&path, PATH_METADATA);
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

//	int tamanioBitarray=cantBloques/2;
//	 if(cantBloques % 2 != 0){
//	  tamanioBitarray++;
//	 }

	int tamanioBitarray=cantBloques;

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
	} else {
		PUERTO_ESCUCHA = config_get_int_value(configuracionFS,
				"PUERTO_ESCUCHA");
	}
	if(!config_has_property(configuracionFS, "PATH_METADATA")){
		log_error(loggerFileSystem, "No se encuentra el parametro PATH_METADATA en el archivo de configuracion");
		exit(-1);
	}
	PATH_METADATA = string_new();
	string_append(&PATH_METADATA, config_get_string_value(configuracionFS, "PATH_METADATA"));
	config_destroy(configuracionFS);
}
