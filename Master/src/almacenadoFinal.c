#include "almacenadoFinal.h"

almacenamientoFinal* recibirSolicitudAlmacenadoFinal(){
	almacenamientoFinal * actual = (almacenamientoFinal*)malloc(sizeof(almacenamientoFinal));
	actual->conexion = (conexionNodo*)malloc(sizeof(conexionNodo));

	actual->conexion->nombreNodo = recibirString(socketYAMA);
	actual->conexion->ipNodo = recibirString(socketYAMA);
	actual->conexion->puertoNodo = recibirUInt(socketYAMA);
	actual->resultadoReduccionGlobal = recibirString(socketYAMA);

	return actual;
}



// A WORKER MANDO:
//- nombre resultadoReduccionGlobal
//- pathDondeGuardar