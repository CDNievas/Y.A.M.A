#include "reduccionLocal.h"


//CHEQUEO SI PUEDO HACER LA REDUCCION LOCAL
bool sePuedeHacerReduccionLocal(t_list* listaDelNodo){
	bool reduccionesLocalesTerminadas(administracionYAMA* admin){
		return admin->estado == FINALIZADO || admin->estado == FALLO;
	}
	char* nombreNodo =  obtenerNombreNodo(listaDelNodo);
	log_info(loggerYAMA, "Se prosigue a verificar si se puede llevar a cabo la reduccion local en el Nodo %s.", nombreNodo);
	pthread_mutex_lock(&semTablaEstados);
	int satisfacen = list_all_satisfy(listaDelNodo, (void*)reduccionesLocalesTerminadas);
	pthread_mutex_unlock(&semTablaEstados);
	free(nombreNodo);
	return satisfacen;
}

//CARGO LA REDUCCION LOCAL
int cargarReduccionLocal(int socket, int nroMaster, t_list* listaDelNodo){
	administracionYAMA* admin = generarAdministracion(obtenerJobDeNodo(listaDelNodo), nroMaster, REDUCCION_LOCAL, obtenerNombreTemporalLocal());
	admin->nombreNodo = obtenerNombreNodo(listaDelNodo);
	admin->nroBloqueFile = 0;
	log_info(loggerYAMA, "Se prosigue a hacer la reduccion local en el nodo %s.", admin->nombreNodo);
	//FALTA NRO DE BLOQUE, PQ NO TENGO LA MAS PALIDA IDEA DE QUE TENGO QUE PONER xd
	admin->nroBloque = 0;
	log_info(loggerYAMA, "El nombre del temporal de reduccion local para el nodo %s es %s", admin->nombreNodo, admin->nameFile);
	conexionNodo* conexion = generarConexionNodo();
	conexion->nombreNodo = string_new();
	string_append(&conexion->nombreNodo, admin->nombreNodo);
	//CARGO LA CONEXION
	obtenerIPYPuerto(conexion);
	if(conexion->ipNodo == NULL && conexion->puertoNodo == -1){
		return -1;
	}
	log_info(loggerYAMA, "Se obtuvieron los datos para llevar a cabo las conexiones con los otros nodos.");
	void* temporalesSerializados = serializarInfoReduccionLocal(conexion, admin->nameFile, listaDelNodo);
	log_info(loggerYAMA, "Se prosigue a enviar los datos para la reduccion local al Master %d.", nroMaster);
	sendRemasterizado(socket, REDUCCION_LOCAL, obtenerTamanioInfoReduccionLocal(conexion, admin->nameFile, listaDelNodo), temporalesSerializados);
	actualizarWLRLocal(admin->nombreNodo, list_size(listaDelNodo));
	log_info(loggerYAMA, "Se actualizo el WL del nodo %s.", admin->nombreNodo);
	free(temporalesSerializados);
	pthread_mutex_lock(&semTablaEstados);
	list_add(tablaDeEstados, admin);
	pthread_mutex_unlock(&semTablaEstados);
	log_info(loggerYAMA, "Se agrego la informacion de la reduccion local del master %d en la tabla de estados.", nroMaster);
	liberarConexion(conexion);
	return 1;
}

//TERMINAR REDUCCION LOCAL
void terminarReduccionLocal(int nroMaster, int socketMaster){
	char* nombreNodo = recibirString(socketMaster);
	log_info(loggerYAMA, "La reducciono local terminada es del nodo %s.", nombreNodo);
	bool esReducLocalBuscada(administracionYAMA* admin){
		return admin->etapa == REDUCCION_LOCAL && !strcmp(admin->nombreNodo, nombreNodo) && admin->nroMaster == nroMaster;
	}
	log_info(loggerYAMA, "Se prosigue a actualizar la tabla de estados.");
	pthread_mutex_lock(&semTablaEstados);
	administracionYAMA* admin = list_find(tablaDeEstados, (void*)esReducLocalBuscada);
	admin->estado = FINALIZADO;
	pthread_mutex_unlock(&semTablaEstados);
	log_info(loggerYAMA, "Tabla de estados actualizada.");
	free(nombreNodo);
}

void fallaReduccionLocal(int nroMaster){
	bool esReducLocal(administracionYAMA* admin){
		return admin->nroMaster == nroMaster && admin->etapa == REDUCCION_LOCAL;
	}
	void finalizarReducLocal(administracionYAMA* admin){
		admin->estado = FALLO;
	}
	pthread_mutex_lock(&semTablaEstados);
	t_list* listaDeReducciones = list_filter(tablaDeEstados, (void*)esReducLocal);

	list_iterate(listaDeReducciones, (void*)finalizarReducLocal);
	pthread_mutex_unlock(&semTablaEstados);
}

