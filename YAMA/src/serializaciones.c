#include "serializaciones.h"

//----------------------------------------------SERIALIZACIONES AUXILIARES-------------------------------------//
//uint32_t obtenerTamanioCopia(copia* copia, conexionNodo* conect){
//	return sizeof(int)*4+string_length(copia->nombreNodo)+string_length(conect->ipNodo);
//}

//copia* deserializarCopia(void* copiaSerializada){
//	copia* nuevaCopia = malloc(sizeof(copia));
//	nuevaCopia->nombreNodo = string_new();
//	int tamanioNombre;
//	memcpy(&tamanioNombre, copiaSerializada, sizeof(int));
//	memcpy(nuevaCopia->nombreNodo, copiaSerializada+sizeof(int), tamanioNombre);
//	memcpy(&nuevaCopia->nroBloque, copiaSerializada+sizeof(int)+tamanioNombre, sizeof(int));
//	return nuevaCopia;
//}

//void* serializarCopia(copia* copiaASerializar, conexionNodo* conection){
//	void* copiaSerializada = malloc(obtenerTamanioCopia(copiaASerializar, conection));
//	int posicionActual = 0;
//	int tamanioNombre = string_length(copiaASerializar->nombreNodo);
//	memcpy(copiaSerializada, &tamanioNombre, sizeof(int));
//	posicionActual += sizeof(int);
//	memcpy(copiaSerializada+posicionActual, copiaASerializar->nombreNodo, tamanioNombre);
//	posicionActual += tamanioNombre;
//	int tamanioIP = string_length(conection->ipNodo);
//	memcpy(copiaSerializada+posicionActual, &tamanioIP, sizeof(int));
//	posicionActual += sizeof(int);
//	memcpy(copiaASerializar+posicionActual, conection->ipNodo, tamanioIP);
//	posicionActual += tamanioIP;
//	memcpy(copiaASerializar+posicionActual, &conection->puertoNodo, sizeof(int));
//	posicionActual += sizeof(int);
//	memcpy(copiaSerializada+posicionActual, &copiaASerializar->nroBloque, sizeof(int));
//	return copiaSerializada;
//}

//-----------------------------------------------TRANSFORMACION------------------------------------------------//
//OBTENER TAMANIO DE INFO EN TRANSFORMACION
uint32_t obtenerTamanioInfoTransformacion(t_list* listaInfoNodo){
	uint32_t tamanioInfoNodo =  sizeof(uint32_t), posicion;
	for(posicion = 0; posicion < list_size(listaInfoNodo); posicion++){
		tamanioInfoNodo += sizeof(uint32_t)*6;
		infoNodo* info = list_get(listaInfoNodo, posicion);
		tamanioInfoNodo += string_length(info->conexion->nombreNodo);
		tamanioInfoNodo += string_length(info->conexion->ipNodo);
		tamanioInfoNodo += string_length(info->nombreTemporal);
	}
	return tamanioInfoNodo;
}

//FUNCION SERIALIZACION DE INFO EN TRANSFORMACION
void *serializarInfoTransformacion(t_list* listaInfoNodo){
	uint32_t posicionActual = 0, cantidadDeNodos = list_size(listaInfoNodo), posicion;
	void* infoSerializada = malloc(obtenerTamanioInfoTransformacion(listaInfoNodo));
	//SERIALIZO CANTIDAD DE NODOS
	memcpy(infoSerializada, &cantidadDeNodos, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	for(posicion = 0; posicion < cantidadDeNodos; posicion++){
		infoNodo* informacion = list_get(listaInfoNodo, posicion);
		uint32_t tamanioNombre = string_length(informacion->conexion->nombreNodo);
		memcpy(infoSerializada+posicionActual, &tamanioNombre, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		memcpy(infoSerializada+posicionActual, informacion->conexion->nombreNodo, tamanioNombre);
		posicionActual += tamanioNombre;
		//SERIALIZO DATOS DE CONEXION DEL NODO
		uint32_t tamanioIP = string_length(informacion->conexion->ipNodo);
		memcpy(infoSerializada+posicionActual, &tamanioIP, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		memcpy(infoSerializada+posicionActual, informacion->conexion->ipNodo, tamanioIP);
		posicionActual += tamanioIP;
		memcpy(infoSerializada+posicionActual, &informacion->conexion->puertoNodo, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		//SERIALIZO LOS DATOS DEL ARCHIVO DEL NODO
		memcpy(infoSerializada+posicionActual, &informacion->nroBloque, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		memcpy(infoSerializada+posicionActual, &informacion->bytesOcupados, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		uint32_t tamanioNombreTemporal = string_length(informacion->nombreTemporal);
		memcpy(infoSerializada+posicionActual, &tamanioNombreTemporal, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		memcpy(infoSerializada+posicionActual, informacion->nombreTemporal, tamanioNombreTemporal);
		posicionActual += tamanioNombreTemporal;
	}
	return infoSerializada;
}

//------------------------------------------------------REDUCCION LOCAL----------------------------------------------//
//OBTENER TAMANIO DE INFO EN REDUCCION LOCAL
uint32_t obtenerTamanioInfoReduccionLocal(conexionNodo* conexion, char* nombreTemporal, t_list* listaDelNodo){
	uint32_t tamanioTemporal = 0, posicion;
	//INTS PARA TAMANIO DE: NOMBRE NODO, NOMBRE TEMPORAL, IP, CANTIDAD DE ARCHIVOS TEMPORALES
	tamanioTemporal += sizeof(uint32_t)*4;
	tamanioTemporal += string_length(conexion->nombreNodo);
	tamanioTemporal += string_length(conexion->ipNodo);
	tamanioTemporal += string_length(nombreTemporal);
	for(posicion = 0; posicion < list_size(listaDelNodo); posicion++){
		//TAMANIO PARA TEMPORALES DE TRANSFORMACION (LOS QUE HAY QUE REDUCIR)
		administracionYAMA* admin = list_get(listaDelNodo, posicion);
		tamanioTemporal += sizeof(uint32_t);
		tamanioTemporal += string_length(admin->nameFile);
	}
	return tamanioTemporal;
}

//FUNCION SERIALIZACION DE INFO EN REDUCCION LOCAL
void* serializarInfoReduccionLocal(conexionNodo* conexion, char* nombreTemporal, t_list* listaDelNodo){
	//OBTENGO LOS DATOS DEL NODO
	void* temporalesSerializados = malloc(obtenerTamanioInfoReduccionLocal(conexion, nombreTemporal, listaDelNodo));
	//CANTIDAD DE BLOQUES
	uint32_t cantidadDeTemporales = list_size(listaDelNodo), posicionActual = 0, posicion;
	memcpy(temporalesSerializados, &cantidadDeTemporales, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	//NOMBRE NODO
	int tamanioNombreNodo = string_length(conexion->nombreNodo);
	memcpy(temporalesSerializados+posicionActual, &tamanioNombreNodo, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(temporalesSerializados+posicionActual, conexion->nombreNodo, tamanioNombreNodo);
	posicionActual += tamanioNombreNodo;
	//DATOS DEL NODO (IP Y PUERTO)
	int tamanioIP = string_length(conexion->ipNodo);
	memcpy(temporalesSerializados+posicionActual, &tamanioIP, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(temporalesSerializados+posicionActual, conexion->ipNodo, tamanioIP);
	posicionActual += tamanioIP;
	memcpy(temporalesSerializados+posicionActual, &conexion->puertoNodo, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	//NOMBRE TEMPORAL DE REDUCCION
	int tamanioNombreTemporal = string_length(nombreTemporal);
	memcpy(temporalesSerializados+posicionActual, &tamanioNombreTemporal, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(temporalesSerializados+posicionActual, nombreTemporal, tamanioNombreTemporal);
	posicionActual += tamanioNombreTemporal;
	//SERIALIZO LOS TEMPORALES A REDUCIR
	for(posicion = 0; posicion<cantidadDeTemporales; posicion++){
	  administracionYAMA* admin = list_get(listaDelNodo, posicion);
	  uint32_t tamanioTemporal = string_length(admin->nameFile);
	  memcpy(temporalesSerializados+posicionActual, &tamanioTemporal, sizeof(uint32_t));
	  posicionActual += sizeof(uint32_t);
	  memcpy(temporalesSerializados+posicionActual, admin->nameFile, tamanioTemporal);
	  posicionActual += tamanioTemporal;
	}
	liberarConexion(conexion); //ERROR DE FREE ACA
	return temporalesSerializados;
}


//SERIALIZACION DE REDUCCION GLOBAL
uint32_t obtenerTamanioReduGlobal(administracionYAMA* reduccion, t_list* listaConexiones, t_list* listaNodos){
	uint32_t tamanio = 0, posicion;
	tamanio += string_length(reduccion->nameFile); //TEMPORAL GLOBAL
	tamanio += sizeof(uint32_t); //TAMANIO DEL NOMBRE DEL TEMPORAL GLOBAL
	for(posicion = 0; posicion<list_size(listaNodos); posicion++){
		administracionYAMA* admin = list_get(listaNodos, posicion);
		tamanio += string_length(admin->nameFile); //TEMPORAL LOCAL
		tamanio += sizeof(uint32_t); //TAMANIO DEL NOMBRE DEL TEMPORAL A REDUCIR
		conexionNodo* conect = list_get(listaConexiones, posicion);
		tamanio += string_length(conect->nombreNodo);
		tamanio += string_length(conect->ipNodo);
		tamanio += (sizeof(uint32_t)*3); //TAMANIO DE IP, NOMBRE NODO Y PUERTO
	}
	return tamanio;
}

conexionNodo* obtenerConexionNodoEncargado(char* nombreNodo, t_list* listaDeConexiones){
	bool esNodo(conexionNodo* conect){
		return strcmp(conect->nombreNodo, nombreNodo);
	}
	return list_remove_by_condition(listaDeConexiones, (void*)esNodo);
}

administracionYAMA* obtenerAdminNodoEncargado(char* nombreNodo, t_list* listaAdministracion){
	bool esNodo(administracionYAMA* admin){
		return strcmp(admin->nameFile, nombreNodo);
	}
	return list_remove_by_condition(listaAdministracion, (void*)esNodo);
}

//ENVIO NODO ENCARGADO, NOMBRE TEMPORAL LOCAL, NOMBRE TEMPORAL GLOBAL, CANTIDAD DE NODOS INVOLUCRADOS, CONEXIONES DE LOS NODOS Y SUS RESPECTIVOS TEMPORALES
void* serializarInfoReduccionGlobal(administracionYAMA* reduccion, t_list* listaDeConexiones, t_list* listaDeNodos){
	uint32_t posicionActual = 0, posicion;
	void* infoSerializada = malloc(obtenerTamanioReduGlobal(reduccion, listaDeConexiones, listaDeNodos));
	uint32_t cantidadDeNodos = list_size(listaDeNodos)-1; //EL -1 ES PARA NO TENER EN CUENTA EL NODO QUE LLEVA A CABO LA REDUCCION
	conexionNodo* conect = obtenerConexionNodoEncargado(reduccion->nombreNodo, listaDeConexiones);
	administracionYAMA* adminEncargado =  obtenerAdminNodoEncargado(reduccion->nombreNodo, listaDeNodos);
	//TIENE QUE SER LIST REMOVE
	uint32_t tamanioNombre = string_length(conect->nombreNodo);
	uint32_t tamanioIP = string_length(conect->ipNodo);
	uint32_t tamanioTemporalGlobal = string_length(reduccion->nameFile);
	uint32_t tamanioTemporalLocal = string_length(adminEncargado->nameFile);
	memcpy(infoSerializada, &tamanioNombre, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada+posicionActual, conect->nombreNodo, tamanioNombre);
	posicionActual += tamanioNombre;
	memcpy(infoSerializada+posicionActual, &tamanioIP, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada+posicionActual, &conect->puertoNodo, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada+posicionActual, &cantidadDeNodos, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada+posicionActual, &tamanioTemporalLocal, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada+posicionActual, adminEncargado->nameFile, tamanioTemporalLocal);
	posicionActual += tamanioTemporalLocal;
	memcpy(infoSerializada+posicionActual, &tamanioTemporalGlobal, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(infoSerializada+posicionActual, reduccion->nameFile, tamanioTemporalGlobal);
	posicionActual += tamanioTemporalGlobal;
	for(posicion = 0; posicion < list_size(listaDeNodos); posicion++){
		administracionYAMA* admin = list_get(listaDeNodos, posicion);
		conexionNodo* conectionAux = list_get(listaDeConexiones, posicion);
		uint32_t tamanioNombreAux = string_length(conectionAux->nombreNodo);
		uint32_t tamanioIPAux = string_length(conectionAux->ipNodo);
		uint32_t tamanioFileLocal = string_length(admin->nameFile);
		memcpy(infoSerializada+posicionActual, &tamanioNombreAux, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		memcpy(infoSerializada+posicionActual, conectionAux->nombreNodo, tamanioNombreAux);
		posicionActual += tamanioNombreAux;
		memcpy(infoSerializada+posicionActual, &tamanioIPAux, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		memcpy(infoSerializada+posicionActual, conectionAux->ipNodo, tamanioIPAux);
		posicionActual += tamanioIPAux;
		memcpy(infoSerializada+posicionActual, &conectionAux->puertoNodo, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		memcpy(infoSerializada+posicionActual, &tamanioFileLocal, sizeof(uint32_t));
		posicionActual += sizeof(uint32_t);
		memcpy(infoSerializada+posicionActual, admin->nameFile, tamanioFileLocal);
		posicionActual += tamanioFileLocal;
	}
	return infoSerializada;

}

uint32_t obtenerTamanioInfoAlmacenamientoFinal(conexionNodo* conect){
	uint32_t tamanioMensaje = 0;
	uint32_t tamanioNombre = string_length(conect->nombreNodo);
	uint32_t tamanioIP = string_length(conect->ipNodo);
	tamanioMensaje += sizeof(uint32_t)*3;
	tamanioMensaje += tamanioNombre;
	tamanioMensaje += tamanioIP;
	return tamanioMensaje;
}

void* serializarInfoAlmacenamientoFinal(conexionNodo* conect){
	uint32_t posicionActual = 0;
	void* serializacionAFinal = malloc(obtenerTamanioInfoAlmacenamientoFinal(conect));
	uint32_t tamanioNombre = string_length(conect->nombreNodo);
	uint32_t tamanioIP = string_length(conect->ipNodo);
	memcpy(serializacionAFinal, &tamanioNombre, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(serializacionAFinal+posicionActual, conect->nombreNodo, tamanioNombre);
	posicionActual += tamanioNombre;
	memcpy(serializacionAFinal+posicionActual, &tamanioIP, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(serializacionAFinal+posicionActual, conect->ipNodo, tamanioIP);
	posicionActual += tamanioIP;
	memcpy(serializacionAFinal+posicionActual, &conect->puertoNodo, sizeof(uint32_t));
	return serializacionAFinal;
}
