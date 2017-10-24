
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
	int cantidadDeBloques = recibirUInt(socketFS);
	int i;
	for(i = 0; i < cantidadDeBloques; i++){
		infoDeFs* informacionDeBloque = generarInformacionDeBloque();
		informacionDeBloque->nroBloque = recibirUInt(socketFS);
		informacionDeBloque->copia1->nombreNodo = recibirString(socketFS);
		informacionDeBloque->copia1->nroBloque = recibirUInt(socketFS);
		informacionDeBloque->copia2->nombreNodo = recibirString(socketFS);
		informacionDeBloque->copia2->nroBloque = recibirUInt(socketFS);
		informacionDeBloque->bytesOcupados = recibirUInt(socketFS);
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
		administracionYAMA* nuevaAdministracion = generarAdministracion(numeroDeJobPTarea, nroMaster, TRANSFORMACION, obtenerNombreTemporalTransformacion());
		//CREE LA BASE DE LA ESTRUCTURA DE TRANSFORMACION
		//PASO A ELEGIR LOS NODOS Y CARGARLOS EN LA ESTRUCTURA
		infoDeFs* infoDeBloque = list_get(listaDeBloques, posicion);
		copia* copiaAUsar = list_get(listaDeCopias, posicion);
		log_info(loggerYAMA, "El Nodo elegido es: %s.\nSu archivo temporal sera: %s\nSu numero de bloque a transformar es: %d. Corresponde al bloque del archivo: %d", copiaAUsar->nombreNodo, nuevaAdministracion->nameFile, copiaAUsar->nroBloque, infoDeBloque->nroBloque);
		nuevaAdministracion->nombreNodo = copiaAUsar->nombreNodo; //CARGO LOS DATOS DE LA COPIA EN LA ESTRUCTURA ADMINISTRATIVA
		nuevaAdministracion->nroBloque = copiaAUsar->nroBloque;
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
	uint32_t nroBloque = recibirUInt(socketMaster);
	int buscarNodo(administracionYAMA* adminAChequear){
		return (adminAChequear->nroMaster == nroMaster && adminAChequear->nroBloque == nroBloque && strcmp(adminAChequear->nombreNodo, nombreNodo)  && adminAChequear->etapa == TRANSFORMACION);
	}
	administracionYAMA *adminAModificar = list_find(tablaDeEstados, (void*)buscarNodo);
	adminAModificar->estado = FINALIZADO;
}

t_list* obtenerBloquesFallidos(uint32_t nroMaster, char* nodoFallido){
	bool esFallido(administracionYAMA* admin){
		return admin->nroMaster == nroMaster && strcmp(admin->nombreNodo, nodoFallido);
	}
	return list_filter(tablaDeEstados, (void*)esFallido);
}

void cargarFallo(uint32_t nroMaster, char* nodoFallido){
	t_list* listaDeFallados = obtenerBloquesFallidos(nroMaster, nodoFallido);
	uint32_t posicion;
	for(posicion = 0; posicion < list_size(listaDeFallados); posicion++){
		administracionYAMA* admin = list_get(listaDeFallados, posicion);
		admin->estado = FALLO;
	}
	list_destroy(listaDeFallados);
}

t_list* filtrarTablaFallida(uint32_t nroMaster, char* nodoFallido){
	bool esEntradaFallida(administracionYAMA* admin){
		return nroMaster == admin->nroMaster || strcmp(admin->nombreNodo, nodoFallido) || admin->estado == FALLO;
	}
	t_list* lista = list_filter(tablaDeEstados, (void*)esEntradaFallida);
	return lista;
}

bool hayQueReplanificar(administracionYAMA* admin, t_list* lista){
	bool esBloqueAReplanificar(infoDeFs* info){
		return info->copia1->nroBloque == admin->nroBloque || info->copia2->nroBloque == admin->nroBloque;
	}
	return list_any_satisfy(lista, (void*)esBloqueAReplanificar);
}

infoDeFs* obtenerDatosAReplanificar(administracionYAMA* admin, t_list* listaDeBloques){
	bool buscarDatoAReplanificar(infoDeFs* info){
		return (admin->nroBloque == info->copia1->nroBloque && strcmp(admin->nombreNodo, info->copia1->nombreNodo))
				|| (admin->nroBloque == info->copia2->nroBloque && strcmp(admin->nombreNodo, info->copia2->nombreNodo));
	}
	return list_find(listaDeBloques, (void*)buscarDatoAReplanificar);
}

copia* obtenerCopiaDeReplanificacion(administracionYAMA* adminFallida, infoDeFs* info){
	if(strcmp(adminFallida->nombreNodo, info->copia1->nombreNodo)){
		return info->copia1;
	}else{
		return info->copia2;
	}
}

void cargarReplanificacion(int socketMaster, uint32_t nroMaster, char* nodoFallido, t_list* listaDeBloques){
	bool esBloqueFallido(infoDeFs* info){
		return strcmp(info->copia1->nombreNodo, nodoFallido) || strcmp(info->copia2->nombreNodo, nodoFallido);
	}
	t_list* listaBloquesAReplanificar = list_filter(listaDeBloques, (void*)esBloqueFallido);
	t_list* listaEntradasAReplanificar = filtrarTablaFallida(nroMaster, nodoFallido);
	t_list* listaParaMaster = list_create();
	t_list* listaParaWL = list_create();
	uint32_t posicion;
	for(posicion = 0; posicion < list_size(listaEntradasAReplanificar); posicion++){
		administracionYAMA* adminFallida = list_get(listaEntradasAReplanificar, posicion);
		if(hayQueReplanificar(adminFallida, listaBloquesAReplanificar)){
			infoDeFs* info = obtenerDatosAReplanificar(adminFallida, listaBloquesAReplanificar);
			copia* copiaACargar = obtenerCopiaDeReplanificacion(adminFallida, info);
			administracionYAMA* nuevaTransformacion = generarAdministracion(obtenerJobDeNodo(listaEntradasAReplanificar),nroMaster, TRANSFORMACION, obtenerNombreTemporalTransformacion());
			nuevaTransformacion->nroBloque = copiaACargar->nroBloque;
			nuevaTransformacion->nombreNodo = copiaACargar->nombreNodo;
			infoNodo* datoPMaster = generarInfoParaMaster(nuevaTransformacion, info);
			list_add(listaParaMaster, datoPMaster);
			list_add(tablaDeEstados, nuevaTransformacion);
			list_add(listaParaWL, copiaACargar);
			reducirWL(nodoFallido);
		}
	}
	actualizarWLTransformacion(listaParaWL);
	void* infoReplanificacionSerializada = serializarInfoTransformacion(listaParaMaster);
	sendRemasterizado(socketMaster, REPLANIFICAR, obtenerTamanioInfoTransformacion(listaParaMaster), infoReplanificacionSerializada);
	free(listaParaMaster);
	free(listaBloquesAReplanificar);
	free(listaEntradasAReplanificar);
	free(listaParaWL);
}









