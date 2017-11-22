#include "reduccionGlobal.h"

bool sePuedeHacerReduccionGlobal(int nroMaster){
	bool esDeNodoYEsRedu(administracionYAMA* admin){
		return nroMaster == admin->nroMaster && admin->etapa == REDUCCION_LOCAL;
	}
	bool esReduccionTerminada(administracionYAMA* admin){
		return admin->estado == FINALIZADO && admin->etapa == REDUCCION_LOCAL;
	}
	t_list* listaDeNodos = list_filter(tablaDeEstados, (void*) esDeNodoYEsRedu);
	int sePuede = list_all_satisfy(listaDeNodos, (void*) esReduccionTerminada);
	list_destroy(listaDeNodos);
	return sePuede;
}


t_list *filtrarReduccionesDelNodo(int nroMaster){
	bool esRLocalTerminada(administracionYAMA* admin){
		return admin->etapa == REDUCCION_LOCAL && admin->nroMaster == nroMaster;
	}
	t_list* listaDeMaster = list_filter(tablaDeEstados, (void*)esRLocalTerminada);
	return listaDeMaster;
}

t_list* obtenerConexionesDeNodos(t_list* listaDeMaster, char* nodoEncargado){
	uint32_t posicion;
	t_list* listaDeConexiones = list_create();
	for(posicion = 0; posicion < list_size(listaDeMaster); posicion++){
		administracionYAMA* admin = list_get(listaDeMaster, posicion);
		conexionNodo* conect = generarConexionNodo();
		conect->nombreNodo = string_new();
		string_append(&conect->nombreNodo, admin->nombreNodo);
		obtenerIPYPuerto(conect);
		if(conect->ipNodo == NULL && conect->puertoNodo == -1){
			return NULL;
		}
		list_add(listaDeConexiones, conect);
	}
	return listaDeConexiones;
}

int cargarReduccionGlobal(int socketMaster, int nroMaster, t_list* listaDeMaster){
	administracionYAMA* nuevaReduccionG = generarAdministracion(obtenerJobDeNodo(listaDeMaster),nroMaster, REDUCCION_GLOBAL, obtenerNombreTemporalGlobal());
	nuevaReduccionG->nroBloque = 0;
	log_info(loggerYAMA, "Se prosigue a aplicar el algoritmo de balanceo para elegir el nodo encargado de la reduccion global");
	nuevaReduccionG->nombreNodo = balancearReduccionGlobal(listaDeMaster);
	log_info(loggerYAMA, "El nodo elegido para llevar a cabo la reduccion global es %s.", nuevaReduccionG->nombreNodo);
	t_list* listaDeConexiones = obtenerConexionesDeNodos(listaDeMaster, nuevaReduccionG->nombreNodo);
	if(listaDeConexiones == NULL){
		log_error(loggerYAMA, "Se fallo al tratar de obtener los datos de conexion de los nodos.");
		return -1;
	}
	log_info(loggerYAMA, "Se pidieron los datos para que el master %d lleve a cabo las conexiones con el nodo encargado.");
	actualizarWLRGlobal(nuevaReduccionG->nombreNodo, list_size(listaDeMaster));
	log_info(loggerYAMA, "Se actualizo el WL del nodo %s.", nuevaReduccionG->nombreNodo);
	void* infoGlobalSerializada = serializarInfoReduccionGlobal(nuevaReduccionG, listaDeConexiones, listaDeMaster);
	sendRemasterizado(socketMaster, REDUCCION_GLOBAL, 0, infoGlobalSerializada);
	log_info(loggerYAMA, "Se enviaron los datos para llevar a cabo la reduccion global al master %d.", nroMaster);
	list_add(tablaDeEstados, nuevaReduccionG);
	free(infoGlobalSerializada);
	list_destroy_and_destroy_elements(listaDeConexiones, (void*)liberarConexion);
	return 1;
}

void terminarReduccionGlobal(uint32_t nroMaster){
	bool esReduGlobalMaster(administracionYAMA* admin){
		return admin->nroMaster == nroMaster && admin->etapa == REDUCCION_GLOBAL && admin->estado == EN_PROCESO;
	}
	administracionYAMA* admin = list_find(tablaDeEstados, (void*)esReduGlobalMaster);
	admin->estado = FINALIZADO;
}



int almacenadoFinal(int socketMaster, uint32_t nroMaster){
	char* nodoEncargado = buscarNodoEncargado(nroMaster);
	log_info(loggerYAMA, "El nodo encargado para el almacenamiento final es %s.", nodoEncargado);
	conexionNodo* conect = generarConexionNodo();
	conect->nombreNodo = string_new();
	string_append(&conect->nombreNodo, nodoEncargado);
	obtenerIPYPuerto(conect);
	if(conect->nombreNodo == NULL && conect->puertoNodo == -1){
		return -1;
	}
	log_info(loggerYAMA, "Se prosigue a serializar la informacion para el almacenamiento final del master %d.", nroMaster);
	void* infoAlmacenadoFinal = serializarInfoAlmacenamientoFinal(conect);
	sendRemasterizado(socketMaster, ALMACENAMIENTO_FINAL, obtenerTamanioInfoAlmacenamientoFinal(conect), infoAlmacenadoFinal);
	log_info(loggerYAMA, "Se enviaron los datos para el almacenamiento final al master %d.", nroMaster);
	free(infoAlmacenadoFinal);
	liberarConexion(conect);
	return 1;
}

void reestablecerWL(int nroMaster){
	bool esTransformacion(administracionYAMA* admin){
		return admin->etapa == TRANSFORMACION && admin->estado == FINALIZADO;
	}
	bool esReduLocal(administracionYAMA* admin){
		return admin->etapa == REDUCCION_LOCAL && admin->estado == FINALIZADO;
	}
	t_list* listaTransformaciones;
	t_list* listaReduccionesLocales;

	listaTransformaciones = list_filter(tablaDeEstados, (void*)esTransformacion);
	listaReduccionesLocales = list_filter(tablaDeEstados, (void*)esReduLocal);

	eliminarTrabajosLocales(listaTransformaciones);
	eliminarTrabajosGlobales(nroMaster, listaReduccionesLocales);

	list_destroy(listaTransformaciones);
	list_destroy(listaReduccionesLocales);
}
