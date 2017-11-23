
#include "funcionesDatanode.h"

void cargarDataNode(t_config* configuracionDN){

    if(!config_has_property(configuracionDN, "IP_FILESYSTEM")){
        log_error(loggerDatanode, "El archivo de configuracion no contiene IP_FILESYSTEM");
        exit(-1);
    }else{
        ipFilesystem = string_new();
        string_append(&ipFilesystem, config_get_string_value(configuracionDN, "IP_FILESYSTEM"));
    }
    if(!config_has_property(configuracionDN, "PUERTO_FILESYSTEM")){
    	log_error(loggerDatanode, "El archivo de configuracion no contiene PUERTO_FILESYSTEM");
    	exit(-1);
    }else{
    	puertoFilesystem = config_get_int_value(configuracionDN, "PUERTO_FILESYSTEM");
    }
    if(!config_has_property(configuracionDN, "NOMBRE_NODO")){
    	log_error(loggerDatanode, "El archivo de configuracion no contiene NOMBRE_NODO");
    	exit(-1);
    }else{
        nombreNodo = string_new();
        string_append(&nombreNodo, config_get_string_value(configuracionDN, "NOMBRE_NODO"));
        string_append(&nombreNodo, "\0");
    }
    if(!config_has_property(configuracionDN, "PUERTO_DATANODE")){
    	log_error(loggerDatanode, "El archivo de configuracion no contiene PUERTO_DATANODE");
    	exit(-1);
    }else{
        puertoDatanode = config_get_int_value(configuracionDN, "PUERTO_DATANODE");
    }
    if(!config_has_property(configuracionDN, "RUTA_DATABIN")){
    	log_error(loggerDatanode, "El archivo de configuracion no contiene RUTA_DATABIN");
    	exit(-1);
    }else{
        rutaDatabin = string_new();
        string_append(&rutaDatabin, config_get_string_value(configuracionDN, "RUTA_DATABIN"));
    }
    config_destroy(configuracionDN);
}

void realizarHandshakeFS(uint32_t socketFS){
	sendDeNotificacion(socketFS, ES_DATANODE);
	uint32_t notificacion = recvDeNotificacion(socketFS);
	if(notificacion != ES_FS){
		log_error(loggerDatanode, "La conexion establecida es erronea.");
		exit(-1);
	}
	log_info(loggerDatanode, "Conexion con FileSystem existosa.");
}

void cargarBin(){

	if(stat(rutaDatabin,&infoDatabin) < 0){

		// Error al abrir el archivo
		log_error(loggerDatanode,"Error al tratar de abrir el archivo.");
		exit(-1);

	} else {

		log_info(loggerDatanode,"Archivo binario de %d bytes encontrado", infoDatabin.st_size);

		// Abro el archivo
		uint32_t archivo;
		if((archivo = open(rutaDatabin, O_RDWR)) < 0){

			// Error al abrir el archivo
			log_error(loggerDatanode,"Error al tratar de abrir el archivo.");
			exit(-1);

		}

		// Lo mapeo a memoria
		mapArchivo = mmap(0, infoDatabin.st_size, PROT_WRITE | PROT_READ, MAP_SHARED, archivo, 0);

		if(mapArchivo == MAP_FAILED){
			// Error al mapear
			log_error(loggerDatanode,"Fallo el mmap.");
			exit(-2);

		}

		if(close(archivo) < 0){
			log_error(loggerDatanode,"Fallo al cerrar el archivo.");
			exit(-3);
		}

		cantBloques = infoDatabin.st_size/SIZEBLOQUE;

	}

}

int escribirBloque(uint32_t nroBloque, char * dataBloque, uint32_t cantBytes){

	if(nroBloque >= cantBloques){

		log_warning(loggerDatanode,"Escritura en bloque inexistente numero: %d de %d.",nroBloque, cantBloques);
		return -1;

	} else {

		// Escribo en memoria del mmap
		memcpy(mapArchivo+(nroBloque*SIZEBLOQUE), dataBloque, cantBytes);

		// Actualizo el archivo
		if(msync(mapArchivo,nroBloque*SIZEBLOQUE,MS_SYNC)){

			// Error de msync()
			log_error(loggerDatanode,"Fallo msync().");
			exit(-3);

		}

		log_info(loggerDatanode,"Escritura en bloque numero: %d", nroBloque);

		return 0;

	}

}

void * leerBloque(uint32_t nroBloque, uint32_t cantBytes){

	if(nroBloque >= cantBloques){

		log_warning(loggerDatanode,"Lectura de bloque inexistente numero: %d de %d.",nroBloque,cantBloques);
		return NULL;

	} else {

		void * dataBloque = miMalloc(cantBytes,loggerDatanode,"Fallo en leerBloque()");
		memcpy(dataBloque,mapArchivo+(nroBloque*SIZEBLOQUE),cantBytes);

		return dataBloque;

	}

}

void enviarInfoNodo(uint32_t socket){

	// Nombre nodo
	u_int32_t tamNombreNodo = string_length(nombreNodo);
	u_int32_t tamanioMensaje = sizeof(tamNombreNodo) + string_length(nombreNodo);

	// Cantidad de bloques
	u_int32_t bloques = cantBloques;

	void * mensaje = malloc(tamanioMensaje + sizeof(uint32_t));

	memcpy(mensaje, &tamNombreNodo, sizeof(uint32_t));
	memcpy(mensaje + sizeof(tamNombreNodo), nombreNodo, tamNombreNodo);
	memcpy(mensaje + tamanioMensaje, &bloques, sizeof(uint32_t));

	sendRemasterizado(socket, ENV_INFONODO, tamanioMensaje+sizeof(bloques), mensaje);

	free(mensaje);

	log_info(loggerDatanode,"Informacion de nodo enviada a FS.");

}

char * recvDeBloque(u_int32_t socket){
	uint32_t tamanio = recibirUInt(socket);
	void* string = malloc(tamanio);
	if(recv(socket, string, tamanio, 0) == -1){
		perror("Error al recibir un string.");
		exit(-1);
	}
	char* stringRecibido = string_substring_until(string, tamanio);
	free(string);
	return stringRecibido;
}

