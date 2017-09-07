#include "../../Biblioteca/src/genericas.c"
#include "../../Biblioteca/src/configParser.c"
#include <commons/log.h>
#include <commons/string.h>

#define PARAMETROS {"IP_FILESYSTEM","PUERTO_FILESYSTEM","NOMBRE_NODO","PUERTO_WORKER","PUERTO_DATANODE","RUTA_DATABIN"}
#define LOGFILE "ggwp.log"

/* FUNCIONES DATANODE

[X] Lee archivo de configuracion
[X] Crea archivo Data.bin
[ ]	Se conecta con filesystem a la espera de solicitudes
[X]	Funcion getBloque()
[X] Funcion setBloque()
[X] Logea acciones

*/

int codError; // Variable que se usa para absorber el codigo de error de una funcion
t_log * logger; // Logger
int sizeDataBin; // Guarda el tamaño del Databin
char * pathDataBin; // Guarda el path del databin
int sizeBloque = 1024*1024; // Tamaño del bloque de los .bin

void generarDatabin(){

	char * msg;

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

	log_info(logger,msg);

}


void * getBloque(int numeroBloque){

	FILE *fp = fopen(pathDataBin, "rb");
	
	char * msg;
	int cantBloquesTot = sizeDataBin/sizeBloque;
	int bloquesLeidos = 0;

	void * bloque = malloc(sizeBloque);

	fseek(fp, numeroBloque*sizeBloque, SEEK_SET);

	// Catch de error por si posicion excede tamaño del archivo
	if ((bloquesLeidos = fread(bloque,sizeBloque,1,fp)) == 1){
		
		fclose(fp);
		msg = string_from_format("Se obtuvo informacion del bloque numero: %i", numeroBloque);
		log_info(logger,msg);
		return bloque;

	} else {

		fclose(fp);
		msg = string_from_format("Bloque de archivo inexistente: %i/%i bloques", numeroBloque, cantBloquesTot);
		log_error(logger,msg);
		return NULL;

	}

}

int setBloque(int numeroBloque, void * datos){

	char * msg;
	int cantBloquesTot = sizeDataBin/sizeBloque;

	// Catcheo si bloque pedido excede cantidad total de bloques
	if(numeroBloque < cantBloquesTot){

		FILE *fp = fopen(pathDataBin, "wb");
		fseek(fp, numeroBloque*sizeBloque, SEEK_SET);
		fwrite(datos,sizeBloque,1,fp);
		fclose(fp);
		msg = string_from_format("Se escribio informacion en el bloque numero: %i", numeroBloque);
		log_info(logger,msg);
		return 0;

	} else {

		msg = string_from_format("Bloque de archivo inexistente: %i/%i bloques", numeroBloque, cantBloquesTot);
		log_error(logger,msg);
		return -1;

	}


}

/* Funcion main pide
	- Path archivo de configuracion.ini
	- Tamaño del archivo .bin
*/

int main(int argc, char **argv) {

	char * pathConfiguracion; // Guarda el path de configuracion
	t_config * configuracion; // Estructura de configuracion
	char * parametros[] = PARAMETROS; // Guarda los parametros definidos en un array

	logger = log_create("ggwp.log", "Datanode", 0, 0);

	// Compruebo que no falten argumentos del main
	if(argc != 3){
		codError = -1;
		printf(KRED "Error con los parametros. \n"
				"Sintaxis: archivo.out 'path de archivo de configuracion' 'Tamanio del data.bin' \n" RESET);
		return codError;
	}

	// Identifico y separo los argumentos del main
	pathConfiguracion = argv[1];
	sizeDataBin = strtol(argv[2],NULL,0)*1024*1024;

	// Compruebo existencia del archivo de configuracion en el path
	if(!existeArchivo(pathConfiguracion)){
		codError = -2;
		printf(KRED "El archivo de configuracion no existe. \n" RESET);
		return codError;
	}

	// Creo un puntero a archivo de configuracion
	configuracion = config_create(pathConfiguracion);

	//Imprimo los valores de la configuracion
	codError = imprimirConfiguracion(configuracion,parametros,sizeof(parametros)/sizeof(parametros[0]));
	switch (codError){
		case -3:
			printf(KRED "Error en la cantidad de parámetros en el archivo de configuración. \n" RESET);
			break;
		case -4:
			printf(KRED "Parametro definido en archivo de configuración no corresponde. \n" RESET);
			break;
	}

	// Manejo del archivo .bin
	pathDataBin = config_get_string_value(configuracion,"RUTA_DATABIN");
	generarDatabin();

	// Creo el puntero para manejar datos
	void * datos = malloc(1024*1024);
	int y = 11;
	memcpy(datos, (void *) &y, sizeof(int));
	
	// Guardo en bloque 0
	if(setBloque(0,datos)==-1){
		codError = -5;
		printf(KRED "11- Acceso a bloque inexistente en archivo .bin. \n" RESET);
		return codError;
	}

	if((datos = getBloque(0)) == NULL){
		codError = -5;
		printf(KRED "2- Acceso a bloque inexistente en archivo .bin. \n" RESET);
		return codError; 
	}

	printf("Dato leido: %i \n", *((int *)datos));

	return EXIT_SUCCESS;

}
