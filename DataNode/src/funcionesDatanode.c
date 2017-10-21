
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
    config_destroy(configuracionDN);
}

void realizarHandshakeFS(int socketFS){
	sendDeNotificacion(socketFS, ES_DATANODE);
	int notificacion = recvDeNotificacion(socketFS);
	if(notificacion != ES_FS){
		log_error(loggerDatanode, "La conexion establecida es erronea.");
		exit(-1);
	}
	log_info(loggerDatanode, "Conexion con FileSystem existosa.");
}

t_bitarray * cargarBin(void * bmap){

	// fstat(FileD, &scriptMap);
	if(stat(RUTA_DATABIN,&infoDatabin) < 0){
		// Error al abrir el archivo
		log_error(loggerDatanode,"Error al tratar de abrir el archivo.");
		log_destroy(loggerDatanode);
		exit(-1);

	} else {

		log_info(loggerDatanode,"Archivo binario encontrado");

		// Abro el archivo
		int archivo = open(RUTA_DATABIN, O_RDWR);

		// Lo mapeo a memoria
		bmap = mmap(0, infoDatabin.st_size, PROT_WRITE | PROT_READ, MAP_SHARED, archivo, 0);

		// Creo el bitarray
		int tamBitarray = (infoDatabin.st_size/1024)/1024;
		return bitarray_create_with_mode(bmap,tamBitarray,MSB_FIRST);

	}

}

