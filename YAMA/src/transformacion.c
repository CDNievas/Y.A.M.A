
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
	if(tipoMsj == 0){
		log_error(loggerYAMA, "Error al recibir paquete con informacion de archivo de FileSystem");
		estaFS = false;
		return NULL;
	}else if(tipoMsj == PATH_FILE_INCORRECTO){
		return NULL;
	}else if(tipoMsj == INFO_ARCHIVO_FS){
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
	}else{
		return NULL;
	}

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
  informacion->conexion->nombreNodo = string_new();
  informacion->nroBloque = administracion->nroBloque;
  informacion->bytesOcupados = infoBloque->bytesOcupados;
  string_append(&informacion->conexion->nombreNodo, administracion->nombreNodo);
  string_append(&informacion->nombreTemporal, administracion->nameFile);
  obtenerIPYPuerto(informacion->conexion);
  if(informacion->conexion->ipNodo == NULL && informacion->conexion->puertoNodo == -1){
	  return NULL;
  }
  return informacion;
}

//CARGO LOS DATOS DE LA TRANSFORMACION EN LA TABLA DE ESTADOS
int cargarTransformacion(int socketMaster, int nroMaster, t_list* listaDeBloques, t_list* listaDeCopias){
	int numeroDeJobPTarea = obtenerNumeroDeJob();
	log_info(loggerYAMA, "El master %d tiene el numero de Job %d.", nroMaster, numeroDeJobPTarea);
	t_list* listaDatosPMaster = list_create();
	int posicion;
	for(posicion = 0; posicion<list_size(listaDeBloques); posicion++){
		administracionYAMA* nuevaAdministracion = generarAdministracion(numeroDeJobPTarea, nroMaster, TRANSFORMACION, obtenerNombreTemporalTransformacion());
		//CREE LA BASE DE LA ESTRUCTURA DE TRANSFORMACION
		//PASO A ELEGIR LOS NODOS Y CARGARLOS EN LA ESTRUCTURA
		infoDeFs* infoDeBloque = list_get(listaDeBloques, posicion);
		nuevaAdministracion->nroBloqueFile = infoDeBloque->nroBloque;
		copia* copiaAUsar = list_get(listaDeCopias, posicion);
		log_info(loggerYAMA, "El Nodo elegido es: %s.", copiaAUsar->nombreNodo);
		log_info(loggerYAMA, "Su archivo temporal sera: %s.", nuevaAdministracion->nameFile);
		log_info(loggerYAMA, "Su numero de bloque a transformar es: %d. Corresponde al bloque del archivo: %d", copiaAUsar->nroBloque, infoDeBloque->nroBloque);
		nuevaAdministracion->nombreNodo = string_new();
		string_append(&nuevaAdministracion->nombreNodo, copiaAUsar->nombreNodo);
//		nuevaAdministracion->nombreNodo = copiaAUsar->nombreNodo; //CARGO LOS DATOS DE LA COPIA EN LA ESTRUCTURA ADMINISTRATIVA
		nuevaAdministracion->nroBloque = copiaAUsar->nroBloque;
		infoNodo* informacionDeNodo = generarInfoParaMaster(nuevaAdministracion, infoDeBloque);
		if(informacionDeNodo == NULL){
			return -1;
		}
		pthread_mutex_lock(&semTablaEstados);
		list_add(tablaDeEstados, nuevaAdministracion);
		pthread_mutex_unlock(&semTablaEstados);
		list_add(listaDatosPMaster, informacionDeNodo);
		liberarInfoFS(infoDeBloque);
		log_info(loggerYAMA, "Se agrego la operacion a la tabla de estados.");
	}
	void* informacionDeTransformacion =  serializarInfoTransformacion(listaDatosPMaster);
	sendRemasterizado(socketMaster, TRANSFORMACION, obtenerTamanioInfoTransformacion(listaDatosPMaster), informacionDeTransformacion);
	log_info(loggerYAMA, "Se envio correctamente la informacion de la transformacion al master %d", nroMaster);
	list_destroy_and_destroy_elements(listaDatosPMaster, (void*)liberarDatoMaster);
	free(informacionDeTransformacion);
	return 1;
}

//FINALIZO LA TRANSFORMACION
void terminarTransformacion(int nroMaster, int socketMaster, char* nombreNodo){
	uint32_t nroBloque = recibirUInt(socketMaster);
	log_info(loggerYAMA, "La transformacion a finalizar es la del nodo %s en el bloque %d.", nombreNodo, nroBloque);
	int buscarNodo(administracionYAMA* adminAChequear){
		return (adminAChequear->nroMaster == nroMaster && adminAChequear->nroBloque == nroBloque && !strcmp(adminAChequear->nombreNodo, nombreNodo)  && adminAChequear->etapa == TRANSFORMACION);
	}
	pthread_mutex_lock(&semTablaEstados);
	administracionYAMA *adminAModificar = list_find(tablaDeEstados, (void*)buscarNodo);
	adminAModificar->estado = FINALIZADO;
	pthread_mutex_unlock(&semTablaEstados);
	log_info(loggerYAMA, "Se actualizo la tabla de estados, finalizando la transformacion del nodo %s sobre el bloque %d.", nombreNodo, nroBloque);
}

t_list* obtenerBloquesFallidos(uint32_t nroMaster, char* nodoFallido){
	bool esFallido(administracionYAMA* admin){
		return admin->nroMaster == nroMaster && strcmp(admin->nombreNodo, nodoFallido);
	}
	pthread_mutex_lock(&semTablaEstados);
	t_list* listaFiltrada = list_filter(tablaDeEstados, (void*)esFallido);
	pthread_mutex_unlock(&semTablaEstados);
	return listaFiltrada;
}

void cargarFallo(uint32_t nroMaster, char* nodoFallido){
	t_list* listaDeFallados = obtenerBloquesFallidos(nroMaster, nodoFallido);
	uint32_t posicion;

	for(posicion = 0; posicion < list_size(listaDeFallados); posicion++){
		pthread_mutex_lock(&semTablaEstados);
		administracionYAMA* admin = list_get(listaDeFallados, posicion);
		admin->estado = FALLO;
		pthread_mutex_unlock(&semTablaEstados);
	}
	list_destroy(listaDeFallados);
}

t_list* filtrarTablaFallida(uint32_t nroMaster, char* nodoFallido){
	bool esEntradaFallida(administracionYAMA* admin){
		return nroMaster == admin->nroMaster && strcmp(admin->nombreNodo, nodoFallido) == 0 && admin->estado == FALLO && admin->etapa == TRANSFORMACION;
	}
	pthread_mutex_lock(&semTablaEstados);
	t_list* lista = list_filter(tablaDeEstados, (void*)esEntradaFallida);
	pthread_mutex_unlock(&semTablaEstados);
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
		return admin->nroBloqueFile == info->nroBloque;
	}
	return list_remove_by_condition(listaDeBloques, (void*)buscarDatoAReplanificar);
}

copia* obtenerCopiaDeReplanificacion(infoDeFs* info){
	bool esCopia1(nodoSistema* nodo){
		return strcmp(nodo->nombreNodo, info->copia1->nombreNodo) == 0;
	}
	bool esCopia2(nodoSistema* nodo){
		return strcmp(nodo->nombreNodo, info->copia2->nombreNodo) == 0;
	}
	nodoSistema* nodo1 = list_find(nodosSistema, (void*)esCopia1);
	nodoSistema* nodo2 = list_find(nodosSistema, (void*)esCopia2);
	if(nodo1->wl > nodo2->wl){
		return info->copia1;
	}else{
		return info->copia2;
	}
}

bool falloNodo(char* nombreNodo, t_list* listaDeMaster){
	bool esFallado(administracionYAMA* admin){
		return strcmp(admin->nombreNodo, nombreNodo) == 0 && admin->estado == FALLO && admin->etapa == TRANSFORMACION;
	}
	return list_any_satisfy(listaDeMaster, (void*)esFallado);
}

bool chequearCopias(infoDeFs* info, t_list* listaDeEntradas){
	return !falloNodo(info->copia1->nombreNodo, listaDeEntradas) && !falloNodo(info->copia2->nombreNodo, listaDeEntradas);
}

bool puedoReplanificar(uint32_t nroMaster, char* nodoFallido, t_list* listaDeBloques){
/*
  1- Obtengo las entradas pertenecientes al nodo de la tabla
  2- Chequeo si el otro nodo (el que no es el fallido), tambien esta fallido
  3- Si ocurre eso: break, retorno false
  4- Sino, retorno true
*/
  uint32_t posicion;
  t_list* listaDeEntradas = filtrarTablaMaster(nroMaster);
  for (posicion = 0; posicion < list_size(listaDeBloques); posicion++) {
    infoDeFs* info = list_get(listaDeBloques, posicion);
    if(chequearCopias(info, listaDeEntradas)){
      list_destroy(listaDeEntradas);
      return false;
    }
  }
  list_destroy(listaDeEntradas);
  return true;
}

int cargarReplanificacion(int socketMaster, uint32_t nroMaster, char* nodoFallido, t_list* listaDeBloques){
	t_list* listaEntradasAReplanificar = filtrarTablaFallida(nroMaster, nodoFallido);
	bool esBloqueFallido(infoDeFs* info){
		bool esInfoFallida(administracionYAMA* admin){
			return admin->nroBloqueFile == info->nroBloque;
		}
		administracionYAMA* admin = list_find(listaEntradasAReplanificar, (void*)esInfoFallida);
		return admin != NULL;
	}
	t_list* listaBloquesAReplanificar = list_filter(listaDeBloques, (void*)esBloqueFallido);
	if(puedoReplanificar(nroMaster, nodoFallido, listaBloquesAReplanificar)){
		t_list* listaParaMaster = list_create();
		t_list* listaParaWL = list_create();
		uint32_t posicion;
		for(posicion = 0; posicion < list_size(listaEntradasAReplanificar); posicion++){
			administracionYAMA* adminFallida = list_get(listaEntradasAReplanificar, posicion);
			infoDeFs* info = obtenerDatosAReplanificar(adminFallida, listaBloquesAReplanificar);
			copia* copiaACargar = obtenerCopiaDeReplanificacion(info);
			administracionYAMA* nuevaTransformacion = generarAdministracion(obtenerJobDeNodo(listaEntradasAReplanificar),nroMaster, TRANSFORMACION, obtenerNombreTemporalTransformacion());
			nuevaTransformacion->nroBloque = copiaACargar->nroBloque;
			nuevaTransformacion->nombreNodo = copiaACargar->nombreNodo;
			nuevaTransformacion->nroBloqueFile = info->nroBloque;
			infoNodo* datoPMaster = generarInfoParaMaster(nuevaTransformacion, info);
			if(datoPMaster == NULL){
				list_destroy(listaParaMaster);
				list_destroy(listaBloquesAReplanificar);
				list_destroy(listaEntradasAReplanificar);
				list_destroy(listaParaWL);
				return -1;
			}
			list_add(listaParaMaster, datoPMaster);
			pthread_mutex_lock(&semTablaEstados);
			list_add(tablaDeEstados, nuevaTransformacion);
			pthread_mutex_unlock(&semTablaEstados);
			list_add(listaParaWL, copiaACargar);
			reducirWL(nodoFallido);
		}
		actualizarWLTransformacion(listaParaWL);
		void* infoReplanificacionSerializada = serializarInfoTransformacion(listaParaMaster);
		sendRemasterizado(socketMaster, REPLANIFICAR, obtenerTamanioInfoTransformacion(listaParaMaster), infoReplanificacionSerializada);
		free(infoReplanificacionSerializada);
		list_destroy(listaParaMaster);
		list_destroy(listaBloquesAReplanificar);
		list_destroy(listaEntradasAReplanificar);
		list_destroy(listaParaWL);
		return 1;
	}else{
		list_destroy(listaBloquesAReplanificar);
		list_destroy(listaEntradasAReplanificar);
		return 0;
	}
}







