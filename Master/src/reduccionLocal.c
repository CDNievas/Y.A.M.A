#include "reduccionLocal.h"

infoReduccionLocal* recibirSolicitudReduccionLocal(){
	infoReduccionLocal* solicitud = malloc(sizeof(infoReduccionLocal));
	solicitud->conexion = (conexionNodo*) malloc(sizeof(conexionNodo));
	solicitud->temporalesTransformacion = list_create();

	uint32_t cantidadTemporales = recibirUInt(socketYama);
	int i;

	solicitud->conexion->nombreNodo = recibirString(socketYama);
	solicitud->conexion->ipNodo = recibirString(socketYama);
	solicitud->conexion->puertoNodo = recibirUInt(socketYama);
	solicitud->temporalReduccionLocal = recibirString(socketYama);

	for(i=0; i<cantidadTemporales;i++){
		char* temporal = recibirString(socketYama);
		list_add(solicitud->temporalesTransformacion,temporal);
	}

	return solicitud;
}
