
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
	int tipoMsj = recvDeNotificacion(socketFS);
	if(tipoMsj != INFO_ARCHIVO_FS){
		log_error(loggerYAMA, "Error al recibir paquete con informacion de archivo de FileSystem");
		exit(-1);
	}
	t_list *listaInfoFs = list_create();
	int cantidadDeBloques = recibirInt(socketFS);
	int i;
	for(i = 0; i < cantidadDeBloques; i++){
		infoDeFs* informacionDeBloque = generarInformacionDeBloque();
		informacionDeBloque->nroBloque = recibirInt(socketFS);
		informacionDeBloque->copia1->nombreNodo = recibirString(socketFS);
		informacionDeBloque->copia1->nroBloque = recibirInt(socketFS);
		informacionDeBloque->copia2->nombreNodo = recibirString(socketFS);
		informacionDeBloque->copia2->nroBloque = recibirInt(socketFS);
		informacionDeBloque->bytesOcupados = recibirInt(socketFS);
		list_add(listaInfoFs, informacionDeBloque);
	}
	return listaInfoFs;
}


char* recibirNombreArchivo(int socketMaster){
	char* nombreArchivo = recibirString(socketMaster);
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

//CARGO LOS DATOS DE LA TRANSFORMACION EN LA TABLA DE ESTADOS
void cargarTransformacion(int socketMaster, int nroMaster, t_list* listaDeBloques, t_list* listaDeCopias){
	int numeroDeJobPTarea = obtenerNumeroDeJob();
	log_info(loggerYAMA, "El master %d tiene el numero de Job %d.", nroMaster, numeroDeJobPTarea);
	t_list* listaDatosPMaster = list_create();
	int posicion;
	log_info(loggerYAMA, "Se prosigue a apligar el algoritmo %s para obtener los Nodos a utilizar.", ALGORITMO_BALANCEO);
	for(posicion = 0; posicion<list_size(listaDeBloques); posicion++){
		administracionYAMA* nuevaAdministracion = generarAdministracion();
		nuevaAdministracion->nroJob = numeroDeJobPTarea;
		nuevaAdministracion->nroMaster = nroMaster;
		nuevaAdministracion->etapa = TRANSFORMACION;
		nuevaAdministracion->nroBloque = posicion;
		nuevaAdministracion->nameFile = obtenerNombreTemporalTransformacion();
		//CREE LA BASE DE LA ESTRUCTURA DE TRANSFORMACION
		//PASO A ELEGIR LOS NODOS Y CARGARLOS EN LA ESTRUCTURA
		infoDeFs* infoDeBloque = list_get(listaDeBloques, posicion);
		copia* copiaAUsar = list_get(listaDeCopias, posicion);
		log_info(loggerYAMA, "El Nodo elegido es: %s.\nSu archivo temporal sera: %s\nSu numero de bloque a transformar es: %d. Corresponde al bloque del archivo: %d", copiaAUsar->nombreNodo, nuevaAdministracion->nameFile, copiaAUsar->nroBloque, infoDeBloque->nroBloque);
		nuevaAdministracion->nombreNodo = copiaAUsar->nombreNodo; //CARGO LOS DATOS DE LA COPIA EN LA ESTRUCTURA ADMINISTRATIVA
		infoNodo* informacionDeNodo = generarInfoParaMaster(nuevaAdministracion, infoDeBloque);
		list_add(tablaDeEstados, nuevaAdministracion);
		list_add(listaDatosPMaster, informacionDeNodo);
		liberarInfoFS(infoDeBloque);
		log_info(loggerYAMA, "Se agrego la operacion a la tabla de estados.");
	}
	void* informacionDeTransformacion =  serializarInfoTransformacion(listaDatosPMaster);
	sendRemasterizado(socketMaster, TRANSFORMACION, obtenerTamanioInfoTransformacion(listaDatosPMaster), informacionDeTransformacion);
	log_info(loggerYAMA, "Se envio correctamente la informacion de la transformacion al master %d", nroMaster);
	list_destroy(listaDatosPMaster);
	free(informacionDeTransformacion);
}

//FINALIZO LA TRANSFORMACION
void terminarTransformacion(int nroMaster, int socketMaster){
	char* nombreNodo = recibirString(socketMaster);
	int nroBloque = recibirInt(socketMaster);
	int buscarNodo(administracionYAMA* adminAChequear){
		return (adminAChequear->nroMaster == nroMaster && adminAChequear->nroBloque == nroBloque && strcmp(adminAChequear->nombreNodo, nombreNodo));
	}
	administracionYAMA *adminAModificar = list_find(tablaDeEstados, (void*)buscarNodo);
	adminAModificar->estado = FINALIZADO;
}
