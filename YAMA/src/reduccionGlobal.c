
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

void cargarReduccionGlobal(int socketMaster, int nroMaster, t_list* listaDeMaster){
	administracionYAMA* nuevaReduccionG = generarAdministracion();
	nuevaReduccionG->nroJob = obtenerJobDeNodo(listaDeMaster);
	nuevaReduccionG->nroBloque = 0;
	nuevaReduccionG->estado = EN_PROCESO;
	nuevaReduccionG->etapa = REDUCCION_GLOBAL;
	nuevaReduccionG->nameFile = obtenerNombreTemporalGlobal();
	nuevaReduccionG->nroMaster = nroMaster;
	nuevaReduccionG->nombreNodo = balancearReduccionGlobal(listaDeMaster);
//	void* infoGlobalSerializada = serializarInfoReduccionGlobal(nuevaReduccionG, listaDeMaster);
//	sendRemasterizado(socketMaster, REDUCCION_GLOBAL, 0, infoGlobalSerializada);
//	free(infoGlobalSerializada);
}
