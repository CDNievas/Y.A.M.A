#include "reduccionLocal.h"


//CHEQUEO SI PUEDO HACER LA REDUCCION LOCAL
bool sePuedeHacerReduccionLocal(t_list* listaDelNodo){
	bool reduccionesLocalesTerminadas(administracionYAMA* admin){
		return admin->estado == FINALIZADO;
	}
	return list_all_satisfy(listaDelNodo, (void*)reduccionesLocalesTerminadas);
}

//CARGO LA REDUCCION LOCAL
void cargarReduccionLocal(int socket, int nroMaster, t_list* listaDelNodo){
	administracionYAMA* admin = generarAdministracion();
	admin->nombreNodo = obtenerNombreNodo(listaDelNodo);
	admin->nroMaster = socket;
	admin->etapa = REDUCCION_LOCAL;
	//FALTA NRO DE BLOQUE, PQ NO TENGO LA MAS PALIDA IDEA DE QUE TENGO QUE PONER xd
	admin->nameFile = obtenerNombreTemporalLocal();
	admin->nroJob = obtenerJobDeNodo(listaDelNodo);
	conexionNodo* conexion = generarConexionNodo();
	string_append(&conexion->nombreNodo, admin->nombreNodo);
	void* temporalesSerializados = serializarInfoReduccionLocal(conexion, admin->nameFile, listaDelNodo);
	int tamanioTemporales = obtenerTamanioInfoReduccionLocal(conexion, admin->nameFile, listaDelNodo);
	sendRemasterizado(socket, REDUCCION_LOCAL, tamanioTemporales, temporalesSerializados);
	free(temporalesSerializados);
	list_add(tablaDeEstados, admin);
}

//TERMINAR REDUCCION LOCAL
void terminarReduccionLocal(int nroMaster, void* mensaje){
	int tamanioNombreNodo;
	memcpy(&tamanioNombreNodo, mensaje, sizeof(int));
	char* nombreNodo = string_new();
	memcpy(nombreNodo, mensaje+sizeof(int), tamanioNombreNodo);
	bool esReducLocalBuscada(administracionYAMA* admin){
		return admin->etapa == REDUCCION_LOCAL && strcmp(admin->nombreNodo, nombreNodo) && admin->nroMaster == nroMaster;
	}
	administracionYAMA* admin = list_find(tablaDeEstados, (void*)esReducLocalBuscada);
	admin->estado = FINALIZADO;
}

