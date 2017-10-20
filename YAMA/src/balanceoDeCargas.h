/*
 * balanceoDeCargas.h
 *
 *  Created on: 22/9/2017
 *      Author: utnso
 */

#include "estructuras.h"
#include "serializaciones.h"
#include "funcionesYAMA.h"

#ifndef BALANCEODECARGAS_H_
#define BALANCEODECARGAS_H_

//UNA VEZ QUE TERMINO DE ELEGIR LOS NODOS, ACTUALIZO LOS WL
void actualizarWL(t_list*);
//BUSCO EL BLOQUE SI LO TIENE OTRO NODO
datosBalanceo* buscarBloque(t_list*, infoDeFs*, int);
//CHEQUEO SI LA TIENE OTRO NODO
bool laTieneOtroNodo(infoDeFs*, t_list*);
//OBTENGO LA COPIA QUE TIENE EL BLOQUE Y QUE VOY A USAR
copia* obtenerCopia(datosBalanceo*, infoDeFs*);
//CHEQUEO SI EL NODO TIENE LA COPIA
bool tieneBloqueBuscado(datosBalanceo*, infoDeFs*);
//CHEQUEO AVAILABILITY (DENTRO DE BALANCEAR TRANSFORMACION)
bool tieneAvailability(datosBalanceo*);
//INCREMENTO LA AVAILABILITY DEL NODO QUE EN LA ETAPA AUXILIAR TUVO EL BLOQUE
void incrementarAvailabilityDeNodo(datosBalanceo*);
//BALANCEO LA TRANSFORMACION, RETORNA LA LISTA DE COPIAS A USAR
t_list* balancearTransformacion(t_list*, t_list*);


char* balancearReduccionGlobal(t_list*);

#endif /* BALANCEODECARGAS_H_ */
