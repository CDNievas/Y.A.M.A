/*
 * consola.c
 *
 *  Created on: 5/12/2017
 *      Author: utnso
 */

#include "consola.h"

void ejecutarComando(uint32_t nro, char ** param){

	switch(nro){


		// FORMAT
		case 1:{
			if(!chequearParamCom(param,1,1)){
				printf("Error en la cantidad de parametros \n");
				log_warning(loggerFileSystem, "Error con los parametros al ejecutar format");
			} else {
				if(estadoAnterior==false){
					uint32_t cantidadNodosSistemas=list_size(tablaNodos->listaNodos);
					if(cantidadNodosSistemas>=2){
						registrarNodosConectados();
						sistemaFormateado = true;
						estadoSeguro=true;
						printf("Se ha formateado correctamente el sistema \n");
						log_info(loggerFileSystem, "Se ha formateado correctamente el sistema");
					}else{
						log_warning(loggerFileSystem,"No hay suficientes DataNode para dejar el FS en un estado Estable");
					}
				} else {
					if(hayUnEstadoEstable()){
						estadoSeguro=true;
						log_info(loggerFileSystem, "Se ha formateado correctamente el sistema");
					} else {
						log_warning(loggerFileSystem,"No hay al menos una copia de cada archivo. Estado no estable.");
					}
				}
			}



		}
		break;


		// RM
		case 2:{

			if(!sistemaFormateado){
				printf("El sistema no se encuentra formateado \n");
				log_warning(loggerFileSystem, "El sistema no se encuentra formateado ");
			} else {

				if(!chequearParamCom(param,2,3)){
					printf("Error en la cantidad de parametros \n");
					log_warning(loggerFileSystem, "Error con los parametros al ejecutar format");
				} else {

					if(strcmp(param[1],"-d")==0){

						int cod = borrarDirectorio(param[2]);

						if(cod == -1){

							printf("Path no pertenece a yamafs \n");
							log_warning(loggerFileSystem, "Path no pertenece a yamafs");

						} else if (cod == -2) {

							printf("Path inexistente \n");
							log_warning(loggerFileSystem, "Path inexistente");

						} else if (cod == -3) {

							printf("No se puede borrar el directorio porque contiene archivos dentro \n");
							log_warning(loggerFileSystem, "No se puede borrar el directorio porque contiene archivos dentro");

						} else {
							printf("Se ha borrado el directorio correctamente \n");
							log_info(loggerFileSystem, "Se ha borrado el directorio correctamente");
						}

					} else {
						//borrarArchivo(param[1]);
					}

				}

			}

		}
		break;



		//MKDIR
		case 6:{

			if(!sistemaFormateado){
				printf("El sistema no se encuentra formateado \n");
				log_warning(loggerFileSystem, "El sistema no se encuentra formateado ");
			} else {

				if(!chequearParamCom(param,2,2)){
					printf("Error en la cantidad de parametros \n");
					log_warning(loggerFileSystem, "Error con los parametros al ejecutar mkdir");
				} else {

					if(!contieneYamafs(param[1])){
						printf("El path no pertenece a yamafs \n");
						log_warning(loggerFileSystem, "El path no pertenece a yamafs");
					} else {

						if(existePath(param[1])){
							printf("Ya existe un directorio con ese nombre \n");
							log_warning(loggerFileSystem, "Ya existe un directorio con ese nombre");
						} else {

							int cod = crearDirectorio(param[1]);

							if(cod == -1){
								printf("Se ha alcanzado el limite de directorios posibles \n");
								log_warning(loggerFileSystem, "Se ha alcanzado el limite de directorios posibles");
							} else if(cod == -2){
								printf("Path incorrecto. Contenia archivos \n");
								log_warning(loggerFileSystem, "Path incorrecto. Contenia archivos");
							} else if(cod == -3){
								printf("Path inexistente \n");
								log_warning(loggerFileSystem, "Path inexistente");
							} else if(cod == -4){
								printf("Ya existe un directorio con ese nombre \n");
								log_warning(loggerFileSystem, "Ya existe un directorio con ese nombre");
							} else {
								printf("Se ha creado el directorio correctamente \n");
								log_info(loggerFileSystem,"Se ha creado el directorio correctamente");
							}

						}

					}

				}

			}

		}
		break;


		//CPFROM
		case 7:{
			if(!sistemaFormateado){
				printf("El sistema no se encuentra formateado \n");
				log_warning(loggerFileSystem, "El sistema no se encuentra formateado ");
			} else {
				estadoSeguro = true;
				if(!estadoSeguro){
					printf("El sistema no se encuentra en un estado seguro \n");
					log_warning(loggerFileSystem, "El sistema no se encuentra en un estado seguro  ");
				} else {

					if(!chequearParamCom(param,4,4)){

						printf("Cantidad incorrecta de parametros \n");
						log_warning(loggerFileSystem,"Error en la cantidad de parametros");

					} else {

						if(!existePathLocal(param[1])){

							printf("El archivo local no existe \n");
							log_warning(loggerFileSystem,"El archivo local no existe");

						} else {

							char * path = string_new();

							char ** pathDesc = string_split(param[1],"/");
							char * nombreArchivo = obtenerNombreUltimoPath(pathDesc);

							string_append(&path,param[2]);
							string_append(&path,nombreArchivo);

							if(existePath(path)){
								printf("Ya existe un archivo con ese nombre \n");
								log_warning(loggerFileSystem,"Ya existe un archivo con ese nombre");

							} else {

								almacenarArchivo(param[1], param[2], param[3]);

							}

							free(path);
							free(pathDesc);

						}

					}

				}

			}
		}
		break;


		// PATH
		case 14:{

			if(!chequearParamCom(param,2,2)){

				printf("Cantidad incorrecta de parametros \n");
				log_warning(loggerFileSystem,"Error en la cantidad de parametros");

			} else if (!contieneYamafs(param[1])) {

				printf("El path no corresponde a yamafs \n");
				log_warning(loggerFileSystem,"Error, el path no corresponde a yamafs");

			} else {

				if(existePath(param[1])){
					printf("El path existe \n");
				} else {
					printf("El path NO existe \n");
				}

			}

		}
		break;

		default:{
			printf("Comando no reconocido \n");
			log_warning(loggerFileSystem,"Comando no reconocido");
		}

	}

}

void consolaFS(){

	size_t maxSize = 100*sizeof(char);
	while(1){
		puts("Esperando comando...");
		printf("->");
		getline(&commandChar, &maxSize, stdin);
		uint32_t recvdSize = strlen(commandChar);
		commandChar[recvdSize-1] = '\0';

		if(strlen(commandChar)==0){
			printf("Comando no reconocido \n");
			log_warning(loggerFileSystem,"Comando no reconocido");
		} else {

			if(strcmp(commandChar, "exit")==0){
				free(commandChar);
				pthread_detach(hiloConsolaFS);
				break;
			}
			analizarComando(commandChar);

		}

	}
}


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

uint32_t countAmmount = 14;

command commands[] = {
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
	{ 12,"info"},
	{ 13,"mkdirN"},
	{ 14,"path"}
};


void analizarComando(char* cmd){

	char** disarmedCmd = string_split(cmd, " ");
	char* function = disarmedCmd[0];
	uint32_t commandNumber = 99;
	command auxCommand;
	int count;
	for(count = 0; count < countAmmount; count++){
		auxCommand = commands[count];
		if(strcmp(function, auxCommand.nombre)==0){
			commandNumber = auxCommand.flag;
			break;
		}
	}

	ejecutarComando(commandNumber,disarmedCmd);

}
