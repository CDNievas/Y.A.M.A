
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

void cargarBin(t_bitarray * bitarray, void * bmap){

	// fstat(FileD, &scriptMap);
	if(stat(RUTA_DATABIN,&infoDatabin) < 0){
		// Error al abrir el archivo
		log_error(loggerDatanode,"Error al tratar de abrir el archivo.");
		log_destroy(loggerDatanode);
		exit(-1);

	} else {

		log_info(loggerDatanode,"Archivo binario encontrado");

		//FileD = open(PATH,O_RDWR);
		int archivo = open(RUTA_DATABIN, O_RDWR);

		printf("%d \n",(int) mmap(NULL, infoDatabin.st_size, PROT_WRITE | PROT_READ, MAP_SHARED, archivo, 0));

		// char* bitmap2 = mmap(0, scriptMap.st_size, PROT_WRITE |PROT_READ , MAP_SHARED, FileD,  0);
		bmap = mmap(NULL, infoDatabin.st_size, PROT_WRITE | PROT_READ, MAP_SHARED, archivo, 0);

		// bitMap= bitarray_create(bitmap2,1048576);
		bitarray = bitarray_create_with_mode(bmap,SIZEBLOQUE,MSB_FIRST);

		//printf("%d",bitarray_get_max_bit(bitarray));

		log_info(loggerDatanode,"Bitarray creado correctamente");

	}


}


/*
void crearBitmap(char  PATH ,char  nodoConectado){
 int FileD;
 FILE* archivoDeBitmap = fopen(PATH,"r+");
 if(archivoDeBitmap == NULL){
  log_info(logFs,"Se tuvo que crear un bitmap nuevo [%s], ya que no habia un bitmap anterior.",nodoConectado);
  archivoDeBitmap =fopen(PATH,"w+");
  int cantidad = 1048576;
  char* cosa = string_repeat('\0',cantidad);
  fwrite(cosa,1,cantidad,archivoDeBitmap);
  free(cosa);
 }
 else{
  log_info(logFs,"Se cargo el bitmap [%s] al FileSystem ",nodoConectado);
 }
 fclose(archivoDeBitmap);
 FileD = open(PATH,O_RDWR);


 struct stat scriptMap;
 fstat(FileD, &scriptMap);

 char* bitmap2 = mmap(0, scriptMap.st_size, PROT_WRITE |PROT_READ , MAP_SHARED, FileD,  0);
 bitMap= bitarray_create(bitmap2,1048576);
 printf("%d",bitarray_get_max_bit(bitmap2));
 log_info(logFs,"[Configurar Todo]-Se creo correctamente el bitmap [%s]",nodoConectado);

}
 */

