
#include "transformacion.h"


//PRIMER PASO: PIDO DATOS DE ARCHIVO A FS (PRIMERO RECIBO DE MASTER EL NOMBRE)
void solicitarArchivo(char* nombreArchivo){
	int tamanioNombreArchivo = string_length(nombreArchivo);
	void* peticionDeArchivo = malloc(sizeof(int)+tamanioNombreArchivo);
	memcpy(peticionDeArchivo, &tamanioNombreArchivo, sizeof(int));
	memcpy(peticionDeArchivo+sizeof(int), nombreArchivo, tamanioNombreArchivo);
	sendRemasterizado(socketFS, INFO_ARCHIVO_FS, sizeof(int)+tamanioNombreArchivo, peticionDeArchivo);
	free(peticionDeArchivo);
}

t_list *recibirInfoArchivo(){
	//RECV REMASTERIZADO CHEQUEAR POR ERROR, QUE SEA TODO LO QUE NECESITO
		paquete* paqueteConInfoArchivo = recvRemasterizado(socketFS);
		if(paqueteConInfoArchivo->tipoMsj != INFO_ARCHIVO_FS){
			log_error(loggerYAMA, "Error al recibir paquete con informacion de archivo de FileSystem");
			exit(-1);
		}
		t_list *listaInfoFs = list_create();
		int cantidadDeBloques;
		memcpy(&cantidadDeBloques, paqueteConInfoArchivo->mensaje, sizeof(int));
		int posicionActual = sizeof(int);
		int i;
		for(i = 0; i < cantidadDeBloques; i++){
		infoDeFs* informacionDeBloque = malloc(sizeof(infoDeFs));
		memcpy(&informacionDeBloque->nroBloque, paqueteConInfoArchivo->mensaje+posicionActual, sizeof(int));
		posicionActual += sizeof(int);
		informacionDeBloque->copia1 = malloc(sizeof(copia));
		informacionDeBloque->copia2 = malloc(sizeof(copia));
		informacionDeBloque->copia1->nombreNodo = string_new();
		informacionDeBloque->copia2->nombreNodo = string_new();
		int tamanioDeNombreCopia1, tamanioDeNombreCopia2;
		memcpy(&tamanioDeNombreCopia1, paqueteConInfoArchivo->mensaje+posicionActual, sizeof(int));
		posicionActual += sizeof(int);
		memcpy(informacionDeBloque->copia1->nombreNodo, paqueteConInfoArchivo->mensaje+posicionActual, tamanioDeNombreCopia1);
		posicionActual += tamanioDeNombreCopia1;
		memcpy(&informacionDeBloque->copia1->nroBloque, paqueteConInfoArchivo->mensaje+posicionActual, sizeof(int));
		posicionActual += sizeof(int);
		memcpy(&tamanioDeNombreCopia2, paqueteConInfoArchivo->mensaje+posicionActual, sizeof(int));
		posicionActual += sizeof(int);
		memcpy(informacionDeBloque->copia2->nombreNodo, paqueteConInfoArchivo->mensaje+posicionActual, tamanioDeNombreCopia2);
		posicionActual += tamanioDeNombreCopia2;
		memcpy(&informacionDeBloque->copia2->nroBloque, paqueteConInfoArchivo->mensaje+posicionActual, sizeof(int));
		posicionActual += sizeof(int);
		memcpy(&informacionDeBloque->bytesOcupados, paqueteConInfoArchivo->mensaje+posicionActual, sizeof(int));
		posicionActual += sizeof(int);
		list_add(listaInfoFs, informacionDeBloque);
	}
	return listaInfoFs;
}


char* recibirNombreArchivo(void* nombreArchivoSerializado){
	int tamanioNombreArchivo;
	memcpy(&tamanioNombreArchivo, nombreArchivoSerializado, sizeof(int));
	char* nombreArchivo = string_new();
	memcpy(nombreArchivo, nombreArchivoSerializado + sizeof(int), tamanioNombreArchivo);
	return nombreArchivo;
}

//GENERO DATOS PARA PODER ENVIARLE A MASTER

infoNodo *generarInfoParaMaster(administracionYAMA* administracion, infoDeFs* infoBloque){
  infoNodo* informacion = malloc(sizeof(infoNodo));
  informacion->conexion = generarConexionNodo();
  informacion->nombreTemporal = string_new();
  informacion->nroBloque = administracion->nroBloque;
  informacion->bytesOcupados = infoBloque->bytesOcupados;
  string_append(&informacion->conexion->nombreNodo, administracion->nombreNodo);
  string_append(&informacion->nombreTemporal, administracion->nameFile);
  obtenerIPYPuerto(informacion->conexion);
  return informacion;
}

//GUARDO LA COPIA A UTILIZAR EN LA TABLA DE ESTADOS

void cargarDatosNodoEnAdmin(administracionYAMA* nuevaAdministracion, copia* copiaAUsar){
	nuevaAdministracion->nombreNodo = copiaAUsar->nombreNodo;
	nuevaAdministracion->nroBloque = copiaAUsar->nroBloque;
}

void cargarTransformacion(int socketMaster, int nroMaster, t_list* listaDeBloques){
	int numeroDeJobPTarea = obtenerNumeroDeJob();
	log_info(loggerYAMA, "El master %d tiene el numero de Job %d.", nroMaster, numeroDeJobPTarea);
	t_list* listaDatosPMaster = list_create();
	int posicion;
	for(posicion = 0; posicion<list_size(listaDeBloques); posicion++){
		administracionYAMA* nuevaAdministracion = generarAdministracion();
		nuevaAdministracion->nroJob = numeroDeJobPTarea;
		nuevaAdministracion->nroMaster = nroMaster;
		nuevaAdministracion->etapa = TRANSFORMACION;
		string_append(&nuevaAdministracion->nameFile, obtenerNombreTemporalTransformacion());
		//CREE LA BASE DE LA ESTRUCTURA DE TRANSFORMACION
		//PASO A ELEGIR LOS NODOS Y CARGARLOS EN LA ESTRUCTURA
		infoDeFs* infoDeBloque = list_get(listaDeBloques, posicion);
		log_info(loggerYAMA, "Se prosigue a apligar el algoritmo %s para obtener el Nodo a utilizar.", ALGORITMO_BALANCEO);
		copia* copiaAUsar = balancearCarga(numeroDeJobPTarea, infoDeBloque); //ELIJO DEPENDIENDO DEL ALGORITMO
		log_info(loggerYAMA, "El Nodo elegido es: %s.\nSu archivo temporal sera: %s\nSu numero de bloque a transformar es: %d. Corresponde al bloque del archivo: %d", copiaAUsar->nombreNodo, nuevaAdministracion->nameFile, copiaAUsar->nroBloque, infoDeBloque->nroBloque);
		cargarDatosNodoEnAdmin(nuevaAdministracion, copiaAUsar); //CARGO LOS DATOS DE LA COPIA EN LA ESTRUCTURA ADMINISTRATIVA
		infoNodo* informacionDeNodo = generarInfoParaMaster(nuevaAdministracion, infoDeBloque);
		list_add(tablaDeEstados, nuevaAdministracion);
		list_add(listaDatosPMaster, informacionDeNodo);
		liberarInfoFS(infoDeBloque);
		log_info(loggerYAMA, "Se agrego la operacion a la tabla de estados.");
	}
	void* informacionDeTransformacion =  serializarInfoTransformacion(listaDatosPMaster);
	sendRemasterizado(socketMaster, TRANSFORMACION, obtenerTamanioInfoTransformacion(listaDatosPMaster), informacionDeTransformacion);
	log_info(loggerYAMA, "Se envio correctamente la informacion de la transformacion al master %d", nroMaster);
	free(informacionDeTransformacion);
}

//FINALIZO LA TRANSFORMACION
void terminarTransformacion(int nroMaster, void* mensaje){
	int tamanioNombre, nroBloque;
	memcpy(&tamanioNombre, mensaje, sizeof(int));
	char* nombreNodo = string_new();
	memcpy(nombreNodo, mensaje+sizeof(int), tamanioNombre);
	memcpy(&nroBloque, mensaje+sizeof(int)+tamanioNombre, sizeof(int));
	int buscarNodo(administracionYAMA* adminAChequear){
		return (adminAChequear->nroMaster == nroMaster && adminAChequear->nroBloque == nroBloque && strcmp(adminAChequear->nombreNodo, nombreNodo));
	}
	administracionYAMA *adminAModificar = list_find(tablaDeEstados, (void*)buscarNodo);
	adminAModificar->estado = FINALIZADO;
}
