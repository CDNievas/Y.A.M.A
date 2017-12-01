
#include "funcionesDatanode.h"

void cargarDataNode(t_config* configuracionDN){

    if(!config_has_property(configuracionDN, "IP_FILESYSTEM")){
        log_error(loggerDatanode, "El archivo de configuracion no contiene IP_FILESYSTEM");
        exit(-1);
    }else{
    	IP_FILESYSTEM = string_new();
        string_append(&IP_FILESYSTEM, config_get_string_value(configuracionDN, "IP_FILESYSTEM"));
    }
    if(!config_has_property(configuracionDN, "PUERTO_FILESYSTEM")){
    	log_error(loggerDatanode, "El archivo de configuracion no contiene PUERTO_FILESYSTEM");
    	exit(-1);
    }else{
    	PUERTO_FILESYSTEM = config_get_int_value(configuracionDN, "PUERTO_FILESYSTEM");
    }
    if(!config_has_property(configuracionDN, "NOMBRE_NODO")){
    	log_error(loggerDatanode, "El archivo de configuracion no contiene NOMBRE_NODO");
    	exit(-1);
    }else{
    	NOMBRE_NODO = string_new();
        string_append(&NOMBRE_NODO, config_get_string_value(configuracionDN, "NOMBRE_NODO"));
        string_append(&NOMBRE_NODO, "\0");
    }
    if(!config_has_property(configuracionDN, "PUERTO_DATANODE")){
    	log_error(loggerDatanode, "El archivo de configuracion no contiene PUERTO_DATANODE");
    	exit(-1);
    }else{
        PUERTO_DATANODE = config_get_int_value(configuracionDN, "PUERTO_DATANODE");
    }
    if(!config_has_property(configuracionDN, "RUTA_DATABIN")){
    	log_error(loggerDatanode, "El archivo de configuracion no contiene RUTA_DATABIN");
    	exit(-1);
    }else{
    	RUTA_DATABIN = string_new();
        string_append(&RUTA_DATABIN, config_get_string_value(configuracionDN, "RUTA_DATABIN"));
    }
    if(!config_has_property(configuracionDN, "PUERTO_WORKER")){
    	log_error(loggerDatanode, "El archivo de configuracion no contiene PUERTO_WORKER");
    	exit(-1);
    }else{
    	PUERTO_WORKER = config_get_int_value(configuracionDN, "PUERTO_WORKER");
    }
    if(!config_has_property(configuracionDN, "IP_WORKER")){
    	log_error(loggerDatanode, "El archivo de configuracion no contiene IP_WORKER");
    	exit(-1);
    }else{
    	IP_WORKER = string_new();
    	string_append(&IP_WORKER, config_get_string_value(configuracionDN, "IP_WORKER"));
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

	if(stat(RUTA_DATABIN,&infoDatabin) < 0){

		// Error al abrir el archivo
		log_error(loggerDatanode,"Error al tratar de abrir el archivo.");
		exit(-1);

	} else {

		log_info(loggerDatanode,"Archivo binario de %d bytes encontrado", infoDatabin.st_size);

		// Abro el archivo
		uint32_t archivo;
		if((archivo = open(RUTA_DATABIN, O_RDWR)) < 0){
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

	if(nroBloque > cantBloques){

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

	u_int32_t tamNombreNodo = string_length(NOMBRE_NODO);
	u_int32_t bloques = cantBloques;
	u_int32_t tamIp = string_length(IP_WORKER);

	int tamanioMsg =sizeof(uint32_t) + tamNombreNodo + sizeof(uint32_t) + sizeof(uint32_t) + tamIp + sizeof(uint32_t);

	void * mensaje = malloc(tamanioMsg);

	int offset = 0;
	memcpy(mensaje + offset, &tamNombreNodo, sizeof(uint32_t));
	offset += sizeof(uint32_t);
	memcpy(mensaje + offset, NOMBRE_NODO, tamNombreNodo);
	offset += tamNombreNodo;
	memcpy(mensaje + offset, &bloques, sizeof(uint32_t));
	offset += sizeof(uint32_t);
	memcpy(mensaje + offset, &tamIp, sizeof(uint32_t));
	offset += sizeof(uint32_t);
	memcpy(mensaje + offset, IP_WORKER, tamIp);
	offset += tamIp;
	memcpy(mensaje + offset, &PUERTO_WORKER, sizeof(uint32_t));

	sendRemasterizado(socket, ENV_INFONODO, tamanioMsg, mensaje);

	free(mensaje);

	log_info(loggerDatanode,"Informacion de nodo enviada a FS.");

}

char * recvDeBloque(u_int32_t socket){
	uint32_t tamanio = recibirUInt(socket);
	void* string = malloc(tamanio);
	if(recv(socket, string, tamanio, MSG_WAITALL) == -1){
		perror("Error al recibir un string.");
		exit(-1);
	}
	char* stringRecibido = string_substring_until(string, tamanio);
	free(string);
	return stringRecibido;
}

void handlerSIGINT(int signal){
	log_info(loggerDatanode, "Se recibio SIGINT. Finalizando proceso");
	munmap(mapArchivo,cantBloques);
	log_destroy(loggerDatanode);
	close(socketServerFS);
	exit(0);
}
