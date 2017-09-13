#include "../../Biblioteca/src/genericas.c"
#include "../../Biblioteca/src/Socket.c"
#include "../../Biblioteca/src/configParser.c"


#define PARAMETROS {"IP_FILESYSTEM","PUERTO_FILESYSTEM","NOMBRE_NODO","PUERTO_DATANODE","RUTA_DATABIN"}
#define LOGFILE "ggwp.log"

/* FUNCIONES DATANODE

[X] Lee archivo de configuracion
[X] Crea archivo Data.bin
[ ]	Se conecta con filesystem a la espera de solicitudes
[X]	Funcion getBloque()
[X] Funcion setBloque()
[X] Logea acciones

*/


int PUERTO_FILESYSTEM;
char* IP_FILESYSTEM;
char* RUTA_DATABIN;
char* NOMBRE_NODO;
int PUERTO_DATANODE;

int codError; // Variable que se usa para absorber el codigo de error de una funcion
t_log * loggerDataNode; // Logger
int sizeDataBin; // Guarda el tamaño del Databin
char * pathDataBin; // Guarda el path del databin
int sizeBloque = 1024*1024; // Tamaño del bloque de los .bin

void cargarDataNode(t_config* configuracionDN){
    if(!config_has_property(configuracionDN, "IP_FILESYSTEM")){
        log_error(loggerDataNode, "El archivo de configuracion no contiene IP_FILESYSTEM");
        exit(-1);
    }else{
        IP_FILESYSTEM = string_new();
        string_append(&IP_FILESYSTEM, config_get_string_value(configuracionDN, "IP_FILESYSTEM"));
    }
    if(!config_has_property(configuracionDN, "PUERTO_FILESYSTEM")){
    	log_error(loggerDataNode, "El archivo de configuracion no contiene PUERTO_FILESYSTEM");
    	exit(-1);
    }else{
        PUERTO_FILESYSTEM = config_get_int_value(configuracionDN, "PUERTO_FILESYSTEM");
    }
    if(!config_has_property(configuracionDN, "NOMBRE_NODO")){
    	log_error(loggerDataNode, "El archivo de configuracion no contiene NOMBRE_NODO");
    	exit(-1);
    }else{
        NOMBRE_NODO = string_new();
        string_append(&NOMBRE_NODO, config_get_string_value(configuracionDN, "NOMBRE_NODO"));
    }
    if(!config_has_property(configuracionDN, "PUERTO_DATANODE")){
    	log_error(loggerDataNode, "El archivo de configuracion no contiene PUERTO_DATANODE");
    	exit(-1);
    }else{
        PUERTO_DATANODE = config_get_int_value(configuracionDN, "PUERTO_DATANODE");
    }
    if(!config_has_property(configuracionDN, "RUTA_DATABIN")){
    	log_error(loggerDataNode, "El archivo de configuracion no contiene RUTA_DATABIN");
    	exit(-1);
    }else{
        RUTA_DATABIN = string_new();
        string_append(&RUTA_DATABIN, config_get_string_value(configuracionDN, "RUTA_DATABIN"));
    }
    config_destroy(configuracionDN);
}


void generarDatabin(){

	char * msg;
	printf("pase");

	// Si no existe el archivo lo creo
	if(!existeArchivo(pathDataBin)){

		FILE *fp = fopen(pathDataBin, "wb");
		ftruncate(fileno(fp), sizeDataBin);
		fclose(fp);
		msg = string_from_format("Se creo el archivo .bin en: %s", pathDataBin);

	// Si existe lo dejo	
	} else {
		msg = string_from_format("Se encontro el archivo .bin en: %s", pathDataBin);
	}

	log_info(loggerDataNode,msg);

}


void * getBloque(int numeroBloque){

	FILE *fp = fopen(pathDataBin, "rb");
	
	int cantBloquesTot = sizeDataBin/sizeBloque;
	int bloquesLeidos = 0;

	void * bloque = malloc(sizeBloque);

	fseek(fp, numeroBloque*sizeBloque, SEEK_SET);

	// Catch de error por si posicion excede tamaño del archivo
	if ((bloquesLeidos = fread(bloque,sizeBloque,1,fp)) == 1){
		
		fclose(fp);
		log_info(loggerDataNode,"Se obtuvo informacion del bloque numero: %i", numeroBloque);
		return bloque;

	} else {

		fclose(fp);
		log_error(loggerDataNode,"Bloque de archivo inexistente: %i/%i bloques", numeroBloque, cantBloquesTot);
		return NULL;

	}

}

int setBloque(int numeroBloque, void * datos){

	int cantBloquesTot = sizeDataBin/sizeBloque;

	// Catcheo si bloque pedido excede cantidad total de bloques
	if(numeroBloque < cantBloquesTot){

		FILE *fp = fopen(pathDataBin, "wb");
		fseek(fp, numeroBloque*sizeBloque, SEEK_SET);
		fwrite(datos,sizeBloque,1,fp);
		fclose(fp);
		log_info(loggerDataNode,"Se escribio informacion en el bloque numero: %i", numeroBloque);
		return 0;

	} else {
		log_error(loggerDataNode,"Bloque de archivo inexistente: %i/%i bloques", numeroBloque, cantBloquesTot);
		return -1;

	}


}

void realizarHandshakeFS(int socketFS){
	sendDeNotificacion(socketFS, ES_DATANODE);
	int notificacion = recvDeNotificacion(socketFS);
	if(notificacion != ES_FS){
		log_error(loggerDataNode, "La conexion establecida es erronea.");
		exit(-1);
	}
	log_info(loggerDataNode, "Conexion con FileSystem existosa.");
}

/* Funcion main pide
	- Path archivo de configuracion.ini
	- Tamaño del archivo .bin
*/

int main(int argc, char **argv) {
	loggerDataNode = log_create("DataNode.log", "DataNode", 1, 0);
	chequearParametros(argc,3);
	t_config* configuracionDataNode = generarTConfig(argv[1], 5);
	//t_config* configuracionDataNode = generarTConfig("Debug/datanode.ini", 5);
	cargarDataNode(configuracionDataNode);
	log_info(loggerDataNode, "Se cargo correctamente DataNode cuyo nombre es %s.", NOMBRE_NODO);
	int socketServerFS = conectarAServer(IP_FILESYSTEM, PUERTO_FILESYSTEM);
	realizarHandshakeFS(socketServerFS);
	while(1){

	}

	return EXIT_SUCCESS;

}
