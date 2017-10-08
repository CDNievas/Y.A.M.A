#include "reduccionLocal.h"


//CHEQUEO SI PUEDO HACER LA REDUCCION LOCAL
bool sePuedeHacerReduccionLocal(t_list* listaDelNodo){
	bool reduccionesLocalesTerminadas(administracionYAMA* admin){
		return admin->estado == FINALIZADO;
	}
	log_info(loggerYAMA, "Se prosigue a verificar si se puede llevar a cabo la reduccion local en el Nodo %s.", obtenerNombreNodo(listaDelNodo));
	return list_all_satisfy(listaDelNodo, (void*)reduccionesLocalesTerminadas);
}

//CARGO LA REDUCCION LOCAL
void cargarReduccionLocal(int socket, int nroMaster, t_list* listaDelNodo){
	administracionYAMA* admin = generarAdministracion();
	admin->nombreNodo = obtenerNombreNodo(listaDelNodo);
	log_info(loggerYAMA, "Se prosigue a hacer la reduccion local en el Nodo %s.", admin->nombreNodo);
	admin->nroMaster = socket;
	admin->etapa = REDUCCION_LOCAL;
	//FALTA NRO DE BLOQUE, PQ NO TENGO LA MAS PALIDA IDEA DE QUE TENGO QUE PONER xd
	admin->nroBloque = 0;
	admin->nameFile = obtenerNombreTemporalLocal();
	log_info(loggerYAMA, "El nombre del temporal de reduccion local para el Nodo %s es %s", admin->nombreNodo, admin->nameFile);
	admin->nroJob = obtenerJobDeNodo(listaDelNodo);
	conexionNodo* conexion = generarConexionNodo();
	conexion->nombreNodo = admin->nombreNodo;
	//CARGO LA CONEXION
	obtenerIPYPuerto(conexion);
	void* temporalesSerializados = serializarInfoReduccionLocal(conexion, admin->nameFile, listaDelNodo);
	int tamanioTemporales = obtenerTamanioInfoReduccionLocal(conexion, admin->nameFile, listaDelNodo);
	log_info(loggerYAMA, "Se prosigue a enviar los datos para la reduccion local al Master %d.", nroMaster);
	sendRemasterizado(socket, REDUCCION_LOCAL, tamanioTemporales, temporalesSerializados);
	free(temporalesSerializados);
	list_add(tablaDeEstados, admin);
	log_info(loggerYAMA, "Se agrego la informacion en la tabla de estados.");
}

//TERMINAR REDUCCION LOCAL
void terminarReduccionLocal(int nroMaster, int socketMaster){
	char* nombreNodo = recibirString(socketMaster);
	bool esReducLocalBuscada(administracionYAMA* admin){
		return admin->etapa == REDUCCION_LOCAL && strcmp(admin->nombreNodo, nombreNodo) && admin->nroMaster == nroMaster;
	}
	administracionYAMA* admin = list_find(tablaDeEstados, (void*)esReducLocalBuscada);
	admin->estado = FINALIZADO;
}

