/*
 * Consola.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "Consola.h"

#define maxSize 256

typedef struct {
	int flag;
	char *nombre;     /* Nombre de la funcion */
} command;


command comandos[] = {
	{ 1,"format"},
	{ 2,"rm"},
	{ 3,"rename"},
	{ 4,"mv"},
	{ 5,"cat"},
	{ 6,"mkdir"},
	{ 7,"cpfrom"},
	{ 8,"cpto"},
	{ 9,"cpblock"},
	{ 10,"md5"},
	{ 11,"ls"},
	{ 12,"info"}
};


void imprimirComandos(){
	puts("\n");
	puts("*************************************************\n");
	puts("*                                               *\n");
	puts("*   Elegir para ejecutar dicho comando:         *\n");
	puts("*                                               *\n");
	puts("*************************************************\n");
	puts("\n");

	puts("format: Formatear el FileSystem. \n");
	puts("rm: Eliminar archivo/directorio/nodo/bloque. \n");
	puts("rename: Renombrar un archivo o directorio. \n");
	puts("mv: Mover un archivo o directorio. \n");
	puts("cat: Mostrar el contenido del archivo como texto plano. \n");
	puts("mkdir: Crear un directorio. \n");
	puts("cpfrom: Copiar un archivo local al yamafs. \n");
	puts("cpto: Copiar un archivo local al yamafs. \n");
	puts("cpblock: Crear copia de un bloque de un archivo. \n");
	puts("md5: Solicitar MD5 de un archivo del yamafs. \n");
	puts("ls Listar los archivos de un directorio. \n");
	puts("info: Mostrar información de un archivo. \n");
}


void analizarComando(char * linea){

	int i;
	int comandoNativo;
	int limite = (sizeof(comandos)/sizeof(comandos[0]));
	command comandoAux;

	char ** comandoDesarmado = string_split(linea," ");
	char * primerPos = comandoDesarmado[0];

	for(i=0; i < limite; i++){

		comandoAux = comandos[i];

		if( strcmp(primerPos, comandoAux.nombre) == 0){
			comandoNativo = comandoAux.flag;
			break;
		}

	}


	switch(comandoNativo){

		case 1:{

			char * comandoNuevo = string_new();
			printf("Formateando FileSystem.\n");

			if(hayEstadoAnterior==false){

				uint cantidadNodosSistemas=list_size(tablaGlobalNodos->nodo);
				if(cantidadNodosSistemas>=2){
					estaFormateado=true;
					esEstadoSeguro=true;
				}else{
					log_error(loggerFileSystem,"No hay suficientes DataNode para dejar el FS en un estado Estable");
				}

			} else {

				if(hayUnEstadoEstable()){
					esEstadoSeguro=true;
				} else {
					log_error(loggerFileSystem,"No hay al menos una copia de cada archivo. Estado no estable.");
				}

			}

			free(comandoNuevo);

		}
        break;

		case 2:{

			if(strcmp(comandoDesarmado[1], "-d")==0){

				int pudoBorrar = deleteDirectory(comandoDesarmado[2]);

				if(pudoBorrar == 0){

					log_error(loggerFileSystem, "El directorio a borrar no existe.");

				} else if(pudoBorrar == -1) {

					log_error(loggerFileSystem, "El directorio a borrar tiene subdirectorios. No se puede borrar.");

				}else{

					char* comandoPConsola = string_new();
					string_append(&comandoPConsola, "rmdir ");
					string_append(&comandoPConsola, comandoDesarmado[2]);
					system(comandoPConsola);
					persistirDirectorio();
					log_info(loggerFileSystem, "Directorio borrado exitosamente.");
					free(comandoPConsola);

				}

			}else if(strcmp(comandoDesarmado[1], "-b")==0){
				//BORRO BLOQUE
			}else if(comandoDesarmado[1]!=NULL){
				//BORRO ARCHIVO
			}else{
				//log_error
			}

		}
		break;


		case 3:{
			char * comandoNuevo = string_new();

			char * nombreArchivoViejo = comandoDesarmado[1];
			char * nombreArchivoNuevo = comandoDesarmado[2];

			if(nombreArchivoViejo == NULL || nombreArchivoNuevo == NULL){
				log_error(loggerFileSystem, "Faltan parametros para ejecutar el comando rename");
			} else {

				renameDirectory(nombreArchivoViejo,nombreArchivoNuevo);

				string_append(&comandoNuevo,"rename ");
				string_append(&comandoNuevo,nombreArchivoViejo);
				string_append(&comandoNuevo," ");
				string_append(&comandoNuevo,nombreArchivoNuevo);

				system(comandoNuevo);
				printf("\n");

			}

			free(comandoNuevo);

		}
		break;

		case 4:{
			char * comandoNuevo = string_new();

			char * pathOriginal = comandoDesarmado[1];
			char * pathFinal = comandoDesarmado[2];

			if(pathOriginal == NULL || pathFinal == NULL){
				log_error(loggerFileSystem, "Faltan parametros para ejecutar el comando mv");
			} else {

				moveDirectory(pathOriginal,pathFinal);

				string_append(&comandoNuevo,"mv ");
				string_append(&comandoNuevo,pathOriginal);
				string_append(&comandoNuevo," ");
				string_append(&comandoNuevo,pathFinal);

				system(comandoNuevo);
				printf("\n");

			}

			free(comandoNuevo);

		}
		break;

		case 5:{

			char * path = comandoDesarmado[1];

			if(existePath(path)){
				printf("Existe path");
			} else {
				printf("No existe path");
			}
			printf("\n");

		}
		break;

		case 6:{

			char* nuevoDirectorio = comandoDesarmado[1];

			if(nuevoDirectorio != NULL){
				if(!existeDirectory(nuevoDirectorio)){
					if(crearDirectorio(nuevoDirectorio) == 1){
						system(linea);
						printf("\n");
					}else{
						log_error(loggerFileSystem, "No se pudo crear el directorio. Por favor vuelva a intentarlo");
					}
				}else{
					log_error(loggerFileSystem, "No se pudo crear el directorio. El directorio ya existe.");
				}
			}else{
				log_error(loggerFileSystem, "Asegurese de ingresar el nombre del directorio. Por favor vuelva a intentarlo");
			}

		}
		break;

		case 7:{
			char * nombreArchivoViejo = comandoDesarmado[1];
			char * nombreArchivoNuevo = comandoDesarmado[2];
			char * flag = comandoDesarmado[3];
			if(nombreArchivoViejo != NULL && nombreArchivoNuevo != NULL && flag != NULL){
				almacenarArchivo(nombreArchivoViejo,nombreArchivoNuevo,flag);
				log_info(loggerFileSystem,"El archivo ha sido copiado exitosamente.");
			}else{
				log_error(loggerFileSystem, "Faltan parametros para ejecutar el comando cpfrom");
			}

		}
		break;

		case 8:{
			//
			// Ver bien como hacer estos comandos despues
			//
			printf("Comando en arreglo! Todavia no se puede ejecutar! (8)\n");
		}
		break;

		case 9:{
			//
			// Ver bien como hacer estos comandos despues
			//
			printf("Comando en arreglo! Todavia no se puede ejecutar! (9)\n");
		}
		break;

		case 10:{
			char * comandoNuevo = string_new();

			char * nombreArchivoViejo = comandoDesarmado[1];

			if(nombreArchivoViejo == NULL){
				log_error(loggerFileSystem, "Faltan parametros para ejecutar el comando md5sum");
			} else {
				string_append(&comandoNuevo,"md5sum ");
				string_append(&comandoNuevo,nombreArchivoViejo);
				system(comandoNuevo);
				printf("\n");
			}

			free(comandoNuevo);

		}
		break;

		case 11:{
			system(linea);
			printf("\n");
		}
		break;

		case 12:{
			char * comandoNuevo = string_new();
			char * nombreArchivoViejo = string_new();

			string_append(&nombreArchivoViejo, comandoDesarmado[1]);
			if(nombreArchivoViejo == NULL){
				log_error(loggerFileSystem, "Faltan parametros para ejecutar el comando info.");
			} else {

				string_append(&comandoNuevo,"ls -l -h ");
				string_append(&comandoNuevo,nombreArchivoViejo);
				system(comandoNuevo);
				printf("\n");

			}

			free(comandoNuevo);
			free(nombreArchivoViejo);

		}
		break;

		default:
			log_error(loggerFileSystem, "Comando no reconocido.");
			break;

	}

}

void consolaFS(){

	char * linea;
	imprimirComandos();

	printf("\n");

	while(1) {
		linea = (char *) readline(">");

		if(linea)
			add_history(linea);

		if(!strncmp(linea, "exit", 4)) {
			pthread_detach(hiloConsolaFS);
			break;
		} else {
			analizarComando(linea);
		}

	}

}
