
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
		log_error(loggerYAMA, "ERROR - RECIBIR INFORMACION DE ARCHIVO");
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
	log_info(loggerYAMA, "MASTER %d - JOB %d", nroMaster, numeroDeJobPTarea);
	t_list* listaDatosPMaster = list_create();
	int posicion;
	for(posicion = 0; posicion<list_size(listaDeBloques); posicion++){
		administracionYAMA* nuevaAdministracion = generarAdministracion(numeroDeJobPTarea, nroMaster, TRANSFORMACION, obtenerNombreTemporalTransformacion(nroMaster, numeroDeJobPTarea));
		//CREE LA BASE DE LA ESTRUCTURA DE TRANSFORMACION
		//PASO A ELEGIR LOS NODOS Y CARGARLOS EN LA ESTRUCTURA
		infoDeFs* infoDeBloque = list_get(listaDeBloques, posicion);
		nuevaAdministracion->nroBloqueFile = infoDeBloque->nroBloque;
		copia* copiaAUsar = list_get(listaDeCopias, posicion);
		log_trace(loggerYAMA, "NODO ELEGIDO %s - BLOQUE NODO %d - BLOQUE ARCHIVO %d", copiaAUsar->nombreNodo, copiaAUsar->nroBloque, infoDeBloque->nroBloque);
		log_trace(loggerYAMA, "TEMPORAL %s", nuevaAdministracion->nameFile);
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
	//	liberarInfoFS(infoDeBloque);
		log_debug(loggerYAMA, "TABLA DE ESTADOS - TRANSFORMACION AGREGADA.");
	}
	log_info(loggerYAMA, "TRANSFORMACION - SERIALIZACION DE DATOS - MASTER %d", nroMaster);
	void* informacionDeTransformacion =  serializarInfoTransformacion(listaDatosPMaster);
	sendRemasterizado(socketMaster, TRANSFORMACION, obtenerTamanioInfoTransformacion(listaDatosPMaster), informacionDeTransformacion);
	log_debug(loggerYAMA, "TRANSFORMACION - INFORMACION ENVIADA - MASTER %d", nroMaster);
	list_destroy_and_destroy_elements(listaDatosPMaster, (void*)liberarDatoMaster);
	free(informacionDeTransformacion);
	return 1;
}

//FINALIZO LA TRANSFORMACION
void terminarTransformacion(int nroMaster, int socketMaster, char* nombreNodo){
	uint32_t nroBloque = recibirUInt(socketMaster);
	int buscarNodo(administracionYAMA* adminAChequear){
		return (adminAChequear->nroMaster == nroMaster && adminAChequear->nroBloque == nroBloque && strcmp(adminAChequear->nombreNodo, nombreNodo)==0  && adminAChequear->etapa == TRANSFORMACION);
	}
	pthread_mutex_lock(&semTablaEstados);
	administracionYAMA *adminAModificar = list_find(tablaDeEstados, (void*)buscarNodo);
	adminAModificar->estado = FINALIZADO;
	pthread_mutex_unlock(&semTablaEstados);
	log_info(loggerYAMA, "TABLA DE ESTADOS - TRANSFORMACION TERMINADA - NODO %s - BLOQUE %d", nombreNodo, nroBloque);
}

t_list* obtenerBloquesFallidos(uint32_t nroMaster, char* nodoFallido){
	bool esFallido(administracionYAMA* admin){
		return admin->nroMaster == nroMaster && strcmp(admin->nombreNodo, nodoFallido)==0;
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
	return list_find(listaDeBloques, (void*)buscarDatoAReplanificar);
}


copia* obtenerCopiaDeReplanificacion(infoDeFs* info, char* nodoFallido){
	bool esCopia1(nodoSistema* nodo){
		return strcmp(nodo->nombreNodo, info->copia1->nombreNodo) == 0;
	}
	bool esCopia2(nodoSistema* nodo){
		return strcmp(nodo->nombreNodo, info->copia2->nombreNodo) == 0;
	}
	nodoSistema* nodo1 = list_find(nodosSistema, (void*)esCopia1);
	nodoSistema* nodo2 = list_find(nodosSistema, (void*)esCopia2);
	if(nodo1->wl > nodo2->wl && strcmp(info->copia2->nombreNodo, nodoFallido)!=0){
		return info->copia2;
	}else if(nodo1->wl < nodo2->wl && strcmp(info->copia1->nombreNodo, nodoFallido)!=0){
		return info->copia1;
	}else if(strcmp(info->copia1->nombreNodo, nodoFallido)==0){
		return info->copia2;
	}else{
		return info->copia1;
	}
}

bool falloNodo(char* nombreNodo, t_list* listaDeMaster){
	bool esFallado(administracionYAMA* admin){
		return strcmp(admin->nombreNodo, nombreNodo) == 0 && admin->estado == FALLO && admin->etapa == TRANSFORMACION;
	}
	return list_any_satisfy(listaDeMaster, (void*)esFallado);
}

bool chequearCopias(infoDeFs* info, t_list* listaDeEntradas){
	return falloNodo(info->copia1->nombreNodo, listaDeEntradas) && falloNodo(info->copia2->nombreNodo, listaDeEntradas);
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
		log_debug(loggerYAMA, "SE PUEDE REPLANIFICAR");
		t_list* listaParaMaster = list_create();
		t_list* listaParaWL = list_create();
		uint32_t posicion;
		for(posicion = 0; posicion < list_size(listaEntradasAReplanificar); posicion++){
			administracionYAMA* adminFallida = list_get(listaEntradasAReplanificar, posicion);
			infoDeFs* info = obtenerDatosAReplanificar(adminFallida, listaBloquesAReplanificar);
			log_warning(loggerYAMA, "BLOQUE A REPLANIFICAR %d", info->nroBloque);
			copia* copiaACargar = obtenerCopiaDeReplanificacion(info, nodoFallido);
			if(copiaACargar == NULL){
				return -1;
			}
			administracionYAMA* nuevaTransformacion = generarAdministracion(obtenerJobDeNodo(listaEntradasAReplanificar),nroMaster, TRANSFORMACION, obtenerNombreTemporalTransformacion());
			nuevaTransformacion->nroBloque = copiaACargar->nroBloque;
			nuevaTransformacion->nombreNodo = string_new();
			string_append(&nuevaTransformacion->nombreNodo, copiaACargar->nombreNodo);
			nuevaTransformacion->nroBloqueFile = info->nroBloque;
			log_trace(loggerYAMA, "NODO ELEGIDO %s - BLOQUE NODO %d - BLOQUE ARCHIVO %d", nuevaTransformacion->nombreNodo, nuevaTransformacion->nroBloque, nuevaTransformacion->nroBloqueFile);
			infoNodo* datoPMaster = generarInfoParaMaster(nuevaTransformacion, info);
			if(datoPMaster == NULL){
				list_destroy(listaParaMaster);
				//list_destroy(listaBloquesAReplanificar);
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
		log_info(loggerYAMA, "REPLANIFICACION - SERIALIZACION DE DATOS - MASTER %d", nroMaster);
		void* infoReplanificacionSerializada = serializarInfoTransformacion(listaParaMaster);
		sendRemasterizado(socketMaster, TRANSFORMACION, obtenerTamanioInfoTransformacion(listaParaMaster), infoReplanificacionSerializada);
		log_debug(loggerYAMA, "REPLANIFICACION - INFORMACION ENVIADA - MASTER %d", nroMaster);
		free(infoReplanificacionSerializada);
		list_destroy_and_destroy_elements(listaParaMaster, (void*)liberarInfoNodo);
		//list_destroy(listaBloquesAReplanificar);
		list_destroy(listaEntradasAReplanificar);
		list_destroy(listaParaWL);
		list_destroy(listaBloquesAReplanificar);
		return 1;
	}else{
		log_error(loggerYAMA,"ERROR - NO SE PUEDE REPLANIFICAR");
		//list_destroy(listaBloquesAReplanificar);
		list_destroy(listaEntradasAReplanificar);
		list_destroy(listaBloquesAReplanificar);
		return 0;
	}
}
