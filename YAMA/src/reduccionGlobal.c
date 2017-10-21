
#include "reduccionGlobal.h"

bool sePuedeHacerReduccionGlobal(int nroMaster){
	bool esDeNodo(administracionYAMA* admin){
		return nroMaster == admin->nroMaster;
	}
	bool esReduccionTerminada(administracionYAMA* admin){
		return admin->estado == FINALIZADO && admin->etapa == REDUCCION_LOCAL;
	}
	t_list* listaDeNodos = list_filter(tablaDeEstados, (void*) esDeNodo);
	return list_all_satisfy(listaDeNodos, (void*) esReduccionTerminada);
}


t_list *filtrarReduccionesDelNodo(int nroMaster){
	bool esRLocalTerminada(administracionYAMA* admin){
		return admin->etapa == REDUCCION_LOCAL && admin->nroMaster == nroMaster;
	}
	t_list* listaDeMaster = list_filter(tablaDeEstados, (void*)esRLocalTerminada);
	return listaDeMaster;
}

t_list* obtenerConexionesDeNodos(t_list* listaDeMaster){
	uint32_t posicion;
	t_list* listaDeConexiones = list_create();
	for(posicion = 0; posicion < list_size(listaDeMaster); posicion++){
		administracionYAMA* admin = list_get(listaDeMaster, posicion);
		conexionNodo* conect = generarConexionNodo();
		string_append(&conect->nombreNodo, admin->nombreNodo);
		obtenerIPYPuerto(conect);
		list_add(listaDeConexiones, conect);
	}
	return listaDeConexiones;
}

char* cargarReduccionGlobal(int socketMaster, int nroMaster, t_list* listaDeMaster){
	administracionYAMA* nuevaReduccionG = generarAdministracion();
	nuevaReduccionG->nroJob = obtenerJobDeNodo(listaDeMaster);
	nuevaReduccionG->nroBloque = 0;
	nuevaReduccionG->estado = EN_PROCESO;
	nuevaReduccionG->etapa = REDUCCION_GLOBAL;
	nuevaReduccionG->nameFile = obtenerNombreTemporalGlobal();
	nuevaReduccionG->nroMaster = nroMaster;
	nuevaReduccionG->nombreNodo = balancearReduccionGlobal(listaDeMaster);
	t_list* listaDeConexiones = obtenerConexionesDeNodos(listaDeMaster);
	void* infoGlobalSerializada = serializarInfoReduccionGlobal(nuevaReduccionG, listaDeConexiones, listaDeMaster);
	sendRemasterizado(socketMaster, REDUCCION_GLOBAL, 0, infoGlobalSerializada);
	free(infoGlobalSerializada);
	return nuevaReduccionG->nombreNodo;
}

void terminarReduccionGlobal(uint32_t nroMaster){
	bool esReduGlobalMaster(administracionYAMA* admin){
		return admin->nroMaster == nroMaster && admin->etapa == REDUCCION_GLOBAL && admin->estado == EN_PROCESO;
	}
	administracionYAMA* admin = list_find(tablaDeEstados, (void*)esReduGlobalMaster);
	admin->estado = FINALIZADO;
}

void almacenadoFinal(int socketMaster, char* nodoEncargado){
	conexionNodo* conect = generarConexionNodo();
	conect->nombreNodo = nodoEncargado;
	obtenerIPYPuerto(conect);
	void* infoAlmacenadoFinal = serializarInfoAlmacenamientoFinal(conect);
	sendRemasterizado(socketMaster, ALMACENAMIENTO_FINAL, obtenerTamanioInfoAlmacenamientoFinal(conect), infoAlmacenadoFinal);
	free(infoAlmacenadoFinal);
}
