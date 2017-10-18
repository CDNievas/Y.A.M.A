#include "serializaciones.h"

//----------------------------------------------SERIALIZACIONES AUXILIARES-------------------------------------//
int obtenerTamanioCopia(copia* copia, conexionNodo* conect){
	return sizeof(int)*4+string_length(copia->nombreNodo)+string_length(conect->ipNodo);
}

//copia* deserializarCopia(void* copiaSerializada){
//	copia* nuevaCopia = malloc(sizeof(copia));
//	nuevaCopia->nombreNodo = string_new();
//	int tamanioNombre;
//	memcpy(&tamanioNombre, copiaSerializada, sizeof(int));
//	memcpy(nuevaCopia->nombreNodo, copiaSerializada+sizeof(int), tamanioNombre);
//	memcpy(&nuevaCopia->nroBloque, copiaSerializada+sizeof(int)+tamanioNombre, sizeof(int));
//	return nuevaCopia;
//}

void* serializarCopia(copia* copiaASerializar, conexionNodo* conection){
	void* copiaSerializada = malloc(obtenerTamanioCopia(copiaASerializar, conection));
	int posicionActual = 0;
	int tamanioNombre = string_length(copiaASerializar->nombreNodo);
	memcpy(copiaSerializada, &tamanioNombre, sizeof(int));
	posicionActual += sizeof(int);
	memcpy(copiaSerializada+posicionActual, copiaASerializar->nombreNodo, tamanioNombre);
	posicionActual += tamanioNombre;
	int tamanioIP = string_length(conection->ipNodo);
	memcpy(copiaSerializada+posicionActual, &tamanioIP, sizeof(int));
	posicionActual += sizeof(int);
	memcpy(copiaASerializar+posicionActual, conection->ipNodo, tamanioIP);
	posicionActual += tamanioIP;
	memcpy(copiaASerializar+posicionActual, &conection->puertoNodo, sizeof(int));
	posicionActual += sizeof(int);
	memcpy(copiaSerializada+posicionActual, &copiaASerializar->nroBloque, sizeof(int));
	return copiaSerializada;
}

//-----------------------------------------------TRANSFORMACION------------------------------------------------//
//OBTENER TAMANIO DE INFO EN TRANSFORMACION
int obtenerTamanioInfoTransformacion(t_list* listaInfoNodo){
	int tamanioInfoNodo =  sizeof(int), posicion;
	for(posicion = 0; posicion < list_size(listaInfoNodo); posicion++){
		tamanioInfoNodo += sizeof(int)*5;
		tamanioInfoNodo += sizeof(long);
		infoNodo* info = list_get(listaInfoNodo, posicion);
		tamanioInfoNodo += string_length(info->conexion->nombreNodo);
		tamanioInfoNodo += string_length(info->conexion->ipNodo);
		tamanioInfoNodo += string_length(info->nombreTemporal);
	}
	return tamanioInfoNodo;
}

//FUNCION SERIALIZACION DE INFO EN TRANSFORMACION
void *serializarInfoTransformacion(t_list* listaInfoNodo){
	int posicionActual = 0, cantidadDeNodos = list_size(listaInfoNodo), posicion;
	void* infoSerializada = malloc(obtenerTamanioInfoTransformacion(listaInfoNodo));
	//SERIALIZO CANTIDAD DE NODOS
	memcpy(infoSerializada, &cantidadDeNodos, sizeof(int));
	posicionActual += sizeof(int);
	for(posicion = 0; posicion < cantidadDeNodos; posicion++){
		infoNodo* informacion = list_get(listaInfoNodo, posicion);
		int tamanioNombre = string_length(informacion->conexion->nombreNodo);
		memcpy(infoSerializada+posicionActual, &tamanioNombre, sizeof(int));
		posicionActual += sizeof(int);
		memcpy(infoSerializada+posicionActual, informacion->conexion->nombreNodo, tamanioNombre);
		posicionActual += tamanioNombre;
		//SERIALIZO DATOS DE CONEXION DEL NODO
		int tamanioIP = string_length(informacion->conexion->ipNodo);
		memcpy(infoSerializada+posicionActual, &tamanioIP, sizeof(int));
		posicionActual += sizeof(int);
		memcpy(infoSerializada+posicionActual, informacion->conexion->ipNodo, tamanioIP);
		posicionActual += tamanioIP;
		memcpy(infoSerializada+posicionActual, &informacion->conexion->puertoNodo, sizeof(int));
		posicionActual += sizeof(int);
		//SERIALIZO LOS DATOS DEL ARCHIVO DEL NODO
		memcpy(infoSerializada+posicionActual, &informacion->nroBloque, sizeof(int));
		posicionActual += sizeof(int);
		memcpy(infoSerializada+posicionActual, &informacion->bytesOcupados, sizeof(long));
		posicionActual += sizeof(long);
		int tamanioNombreTemporal = string_length(informacion->nombreTemporal);
		memcpy(infoSerializada+posicionActual, &tamanioNombreTemporal, sizeof(int));
		posicionActual += sizeof(int);
		memcpy(infoSerializada+posicionActual, informacion->nombreTemporal, tamanioNombreTemporal);
		posicionActual += tamanioNombreTemporal;
	}
	return infoSerializada;
}

//------------------------------------------------------REDUCCION LOCAL----------------------------------------------//
//OBTENER TAMANIO DE INFO EN REDUCCION LOCAL
int obtenerTamanioInfoReduccionLocal(conexionNodo* conexion, char* nombreTemporal, t_list* listaDelNodo){
	int tamanioTemporal = 0, posicion;
	//INTS PARA TAMANIO DE: NOMBRE NODO, NOMBRE TEMPORAL, IP, CANTIDAD DE ARCHIVOS TEMPORALES
	tamanioTemporal += sizeof(int);
	tamanioTemporal += sizeof(int);
	tamanioTemporal += sizeof(int);
	tamanioTemporal += sizeof(int);
	tamanioTemporal += string_length(conexion->nombreNodo);
	tamanioTemporal += string_length(conexion->ipNodo);
	tamanioTemporal += string_length(nombreTemporal);
	for(posicion = 0; posicion < list_size(listaDelNodo); posicion++){
		//TAMANIO PARA TEMPORALES DE TRANSFORMACION (LOS QUE HAY QUE REDUCIR)
		administracionYAMA* admin = list_get(listaDelNodo, posicion);
		tamanioTemporal += sizeof(int);
		tamanioTemporal += string_length(admin->nameFile);
	}
	return tamanioTemporal;
}

//FUNCION SERIALIZACION DE INFO EN REDUCCION LOCAL
void* serializarInfoReduccionLocal(conexionNodo* conexion, char* nombreTemporal, t_list* listaDelNodo){
	//OBTENGO LOS DATOS DEL NODO
	void* temporalesSerializados = malloc(obtenerTamanioInfoReduccionLocal(conexion, nombreTemporal, listaDelNodo));
	//CANTIDAD DE BLOQUES
	int cantidadDeTemporales = list_size(listaDelNodo), posicionActual = 0, posicion;
	memcpy(temporalesSerializados, &cantidadDeTemporales, sizeof(int));
	posicionActual += sizeof(int);
	//NOMBRE NODO
	int tamanioNombreNodo = string_length(conexion->nombreNodo);
	memcpy(temporalesSerializados+posicionActual, &tamanioNombreNodo, sizeof(int));
	posicionActual += sizeof(int);
	memcpy(temporalesSerializados+posicionActual, conexion->nombreNodo, tamanioNombreNodo);
	posicionActual += tamanioNombreNodo;
	//DATOS DEL NODO (IP Y PUERTO)
	int tamanioIP = string_length(conexion->ipNodo);
	memcpy(temporalesSerializados+posicionActual, &tamanioIP, sizeof(int));
	posicionActual += sizeof(int);
	memcpy(temporalesSerializados+posicionActual, conexion->ipNodo, tamanioIP);
	posicionActual += tamanioIP;
	memcpy(temporalesSerializados+posicionActual, &conexion->puertoNodo, sizeof(int));
	posicionActual += sizeof(int);
	//NOMBRE TEMPORAL DE REDUCCION
	int tamanioNombreTemporal = string_length(nombreTemporal);
	memcpy(temporalesSerializados+posicionActual, &tamanioNombreTemporal, sizeof(int));
	posicionActual += sizeof(int);
	memcpy(temporalesSerializados+posicionActual, nombreTemporal, tamanioNombreTemporal);
	posicionActual += tamanioNombreTemporal;
	//SERIALIZO LOS TEMPORALES A REDUCIR
	for(posicion = 0; posicion<cantidadDeTemporales; posicion++){
	  administracionYAMA* admin = list_get(listaDelNodo, posicion);
	  int tamanioTemporal = string_length(admin->nameFile);
	  memcpy(temporalesSerializados+posicionActual, &tamanioTemporal, sizeof(int));
	  posicionActual += sizeof(int);
	  memcpy(temporalesSerializados+posicionActual, admin->nameFile, tamanioTemporal);
	  posicionActual += tamanioTemporal;
	}
	liberarConexion(conexion); //ERROR DE FREE ACA
	return temporalesSerializados;
}

