#include "reduccionLocal.h"


//CHEQUEO SI PUEDO HACER LA REDUCCION LOCAL
bool sePuedeHacerReduccionLocal(t_list* listaDelNodo){
	bool reduccionesLocalesTerminadas(administracionYAMA* admin){
		return admin->estado == FINALIZADO || admin->estado == FALLO;
	}
	log_info(loggerYAMA, "Se prosigue a verificar si se puede llevar a cabo la reduccion local en el Nodo %s.", obtenerNombreNodo(listaDelNodo));
	return list_all_satisfy(listaDelNodo, (void*)reduccionesLocalesTerminadas);
}

//CARGO LA REDUCCION LOCAL
void cargarReduccionLocal(int socket, int nroMaster, t_list* listaDelNodo){
	administracionYAMA* admin = generarAdministracion(obtenerJobDeNodo(listaDelNodo), nroMaster, REDUCCION_LOCAL, obtenerNombreTemporalLocal());
	admin->nombreNodo = obtenerNombreNodo(listaDelNodo);
	log_info(loggerYAMA, "Se prosigue a hacer la reduccion local en el Nodo %s.", admin->nombreNodo);
	//FALTA NRO DE BLOQUE, PQ NO TENGO LA MAS PALIDA IDEA DE QUE TENGO QUE PONER xd
	admin->nroBloque = 0;
	log_info(loggerYAMA, "El nombre del temporal de reduccion local para el Nodo %s es %s", admin->nombreNodo, admin->nameFile);
	conexionNodo* conexion = generarConexionNodo();
	conexion->nombreNodo = string_new();
	string_append(&conexion->nombreNodo, admin->nombreNodo);
	//CARGO LA CONEXION
	obtenerIPYPuerto(conexion);
	void* temporalesSerializados = serializarInfoReduccionLocal(conexion, admin->nameFile, listaDelNodo);
	log_info(loggerYAMA, "Se prosigue a enviar los datos para la reduccion local al Master %d.", nroMaster);
	sendRemasterizado(socket, REDUCCION_LOCAL, obtenerTamanioInfoReduccionLocal(conexion, admin->nameFile, listaDelNodo), temporalesSerializados);
	actualizarWLRLocal(admin->nombreNodo, list_size(listaDelNodo));
	free(temporalesSerializados);
	list_add(tablaDeEstados, admin);
	log_info(loggerYAMA, "Se agrego la informacion en la tabla de estados.");
	liberarConexion(conexion);
}

//TERMINAR REDUCCION LOCAL
void terminarReduccionLocal(int nroMaster, int socketMaster){
	char* nombreNodo = recibirString(socketMaster);
	bool esReducLocalBuscada(administracionYAMA* admin){
		return admin->etapa == REDUCCION_LOCAL && !strcmp(admin->nombreNodo, nombreNodo) && admin->nroMaster == nroMaster;
	}
	administracionYAMA* admin = list_find(tablaDeEstados, (void*)esReducLocalBuscada);
	admin->estado = FINALIZADO;
	free(nombreNodo);
}

