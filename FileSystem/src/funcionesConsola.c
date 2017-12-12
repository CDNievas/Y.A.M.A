/*
 * funcionesConsola.c
 *
 *  Created on: 7/12/2017
 *      Author: utnso
 */

#include "funcionesConsola.h"


void liberarRutaDesarmada(char** ruta){
	uint32_t cont=0;
	for(;ruta[cont]!=NULL;cont++){
		free(ruta[cont]);
	}
	free(ruta);
}


int cantParam(char ** com){

	int i=0;
	while(com[i] != NULL){
		i++;
	}
	return i;

}

bool chequearParamCom(char ** com, int cantMin, int cantMax){
	int x = cantParam(com);
	return cantMin <= x && x <= cantMax;
}

bool contieneYamafs(char * path){

	char ** pathDesc = string_split(path,"/");

	int i = 0;
	while(pathDesc[i] != NULL){
		if(strcmp(pathDesc[i],"yamafs:") == 0){
			free(pathDesc);
			return true;
		} else {
			i++;
		}
	}
	free(pathDesc);
	return false;
}

uint32_t sacarTamanioArchivo (FILE* archivo){
	fseek(archivo,0,SEEK_END);
	uint32_t tamanio=ftell(archivo);
	return tamanio;
}


bool almacenarArchivo(char* pathArchivo, char* pathDirectorio, char* tipo){
	FILE* archivoALeer=fopen(pathArchivo,"r+");

	//SI NO SE PUEDE ABRIR EL ARCHIVO
	if(archivoALeer==NULL){
		printf("Error al tratar de abrir el archivo \n");
		log_warning(loggerFileSystem,"Error al tratar de abrir el archivo.");
		return false;
	}

	//SI existe el directorio
	if(!existePath(pathDirectorio)){
		printf("El directorio de yamafs no existe \n");
		log_warning(loggerFileSystem,"El directorio de yamafs no existe.");
		return false;
	}

	//Si tiene algo almacenado
	uint32_t tamanioDelArchivo = sacarTamanioArchivo(archivoALeer);
	if(tamanioDelArchivo==0){
		printf("El archivo que se desea guardar no contiene nada \n");
		log_warning(loggerFileSystem,"El archivo que se desea guardar no contiene nada.");
		fclose(archivoALeer);
		return false;
	}

	//VERIFICO SI EL TIPO DE DATO ES EL CORRECTO
	if(strcmp(tipo,"B")!=0 && strcmp(tipo,"T")!=0){
		printf("El tipo de archivo ingresado es incorrecto \n");
		log_warning(loggerFileSystem,"El tipo de archivo ingresado es incorrecto");
		fclose(archivoALeer);
		return false;
	}


	//GENERO LA INSTANCIO DEL ARCHIVO PARA ALMACENARLO
	strArchivo* entradaArchivoAGuardar=malloc(sizeof(strArchivo));
	entradaArchivoAGuardar->tamanio=sacarTamanioArchivo(archivoALeer);
	entradaArchivoAGuardar->bloques=list_create();
	entradaArchivoAGuardar->nombre=string_new();
	entradaArchivoAGuardar->tipo=string_new();

		//OBTENGO EL NOMBRE DEL ARCHIVO
	char** rutaArchivo=string_split(pathArchivo,"/");
	char** rutaDirectorio = string_split(pathDirectorio,"/");
	//TENGO QUE LIBERARLO
	char* nombreArchivo=obtenerNombreUltimoPath(rutaArchivo);
	string_append(&entradaArchivoAGuardar->nombre,nombreArchivo);

		//HAY QUE LIBERAR RUTA ARHICHIVO

		//OBTENGO EL INDEX DEL DIRECTORIO PARDRE
	entradaArchivoAGuardar->directorioPadre=obtenerIdDirectorio(rutaDirectorio,0,-1);

		//OBTENGO EL TIPO
	string_append(&entradaArchivoAGuardar->tipo,tipo);

	liberarRutaDesarmada(rutaArchivo);
	liberarRutaDesarmada(rutaDirectorio);

	t_list* posicionesBloquesAGuardar=list_create();

	log_trace(loggerFileSystem,"Se procede a calcular la cantidad de bloques que ocupa el archivo %s",nombreArchivo);
	if(strcmp(tipo,"B")==0){
		uint32_t tamAux=0;
		while(tamanioDelArchivo>0){
			if(tamanioDelArchivo<1048576){
				tamAux+=tamanioDelArchivo;
				list_add(posicionesBloquesAGuardar,tamAux);
				tamanioDelArchivo-=tamanioDelArchivo;
			}else{
				tamAux+=1048576;
				list_add(posicionesBloquesAGuardar,tamAux);
				tamanioDelArchivo-=1048576;
			}
		}

	}
	if(strcmp(tipo,"T")==0){
		int digito;
		uint32_t ultimoBarraN=0;
		uint32_t registroAntesMega=0;
		uint32_t ultimaPosicion=0;

		fseek(archivoALeer,0,SEEK_SET);
		while(!feof(archivoALeer)){
			digito = fgetc(archivoALeer);
			if(digito=='\n'){
				ultimoBarraN = ftell(archivoALeer);
			}

			if(ftell(archivoALeer)==1048576+registroAntesMega){
				registroAntesMega=ultimoBarraN;
				list_add(posicionesBloquesAGuardar,ultimoBarraN);
			}
		}

		fseek(archivoALeer,0,SEEK_END);
		ultimaPosicion=ftell(archivoALeer);
		list_add(posicionesBloquesAGuardar,ultimaPosicion);
		fseek(archivoALeer,0,SEEK_SET);
	}

	uint32_t cantidadBloquesArchivo=list_size(posicionesBloquesAGuardar);
	if(tablaNodos->tamanioFSLibre<cantidadBloquesArchivo*2){
		printf("El tamaño del archivo supera la capacidad de almacenamiento del sistema \n");
		log_warning(loggerFileSystem,"El tamaño del archivo supera la capacidad de almacenamiento del sistema");
		//DEBO LIBERAR todo
		return false;
	}

	entradaArchivoAGuardar->disponible=true;

	log_trace(loggerFileSystem,"Se Procede almacenar el %s.",nombreArchivo);
	enviarDatosANodo(posicionesBloquesAGuardar,archivoALeer,entradaArchivoAGuardar);
	log_info(loggerFileSystem,"Se ha almacenado correctamente el archivo %s.",nombreArchivo);
	list_add(tablaArchivos,entradaArchivoAGuardar);


	//PERSISTO EL ARCHIVO
	persistirArchivo(entradaArchivoAGuardar);
	//PERSISTO MI TABLA DE NODOS
	persistirTablaNodo();
	list_destroy(posicionesBloquesAGuardar);
	fclose(archivoALeer);
	free(nombreArchivo);
	return true;

}


int crearDirectorio(char * path){

	int indexDir = list_size(tablaDirectorios);
	if(indexDir<=100){

		char ** pathDesc = string_split(path,"/");

		int idPadre = directorioInexistente(pathDesc,0,-1);

		if(idPadre == -2 || idPadre == -3 || idPadre == -4){
			return idPadre;
		} else {

			strDirectorio * directorio = malloc(sizeof(strDirectorio));
			directorio->nombre = string_new();
			string_append(&directorio->nombre,obtenerNombreUltimoPath(pathDesc));
			directorio->padre = idPadre;
			directorio->index = indexDir;

			list_add(tablaDirectorios,directorio);
			persistirTablaDirectorio();
			//free(pathDesc);
			return 0;

		}

	} else {
		return -1;
	}

}

int borrarDirectorio(char * pathDir){

	if(!contieneYamafs(pathDir)){
		return -1;
	} else {

		char ** pathDesc = string_split(pathDir,"/");

		int idDir = obtenerIdDirectorio(pathDesc,0,-1);

		if(idDir == -2){
			return idDir;
		} else {

			bool existenArchivosEnDir(strArchivo * archivo){
				return archivo->directorioPadre==idDir;
			}

			if(list_any_satisfy(tablaArchivos,(void *) existenArchivosEnDir)){

				return -3;

			} else {

				int idPadreDir = obtenerIdPadreDirectorio(pathDesc,0,-1);

				char * nombreDirectorio = obtenerNombreUltimoPath(pathDesc);
				bool eliminarDirectorio(strDirectorio * directorio){
					return (strcmp(directorio->nombre,nombreDirectorio)==0 && directorio->padre==idPadreDir);
				}

				strDirectorio * directorio=list_remove_by_condition(tablaDirectorios,(void*)eliminarDirectorio);

				if(directorio!=NULL){
					free(directorio->nombre);
					free(directorio);
				}

				return 0;

			}

		}

	}

}

