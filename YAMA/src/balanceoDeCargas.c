#include "balanceoDeCargas.h"
#include <math.h>

void incrementarAvailability(t_list* listaBalanceo){
	int posicion;
	for(posicion = 0; posicion < list_size(listaBalanceo); posicion++){
		datosBalanceo* nodo = list_get(listaBalanceo, posicion);
		incrementarAvailabilityDeNodo(nodo);
	}
}

void reducirWL(char* nodo){
	bool esNodo(nodoSistema* aModificar){
		return strcmp(aModificar->nombreNodo, nodo);
	}
	nodoSistema* nodoAModificar = list_find(nodosSistema, (void*)esNodo);
	nodoAModificar->wl--;
}

void actualizarWLTransformacion(t_list* copiasElegidas){
	copia* copiaElegida;
	bool esNodo(nodoSistema* nodo){
		return !strcmp(nodo->nombreNodo, copiaElegida->nombreNodo);

	}
	int posicion;
	for(posicion = 0; posicion < list_size(copiasElegidas); posicion++){
		copiaElegida = list_get(copiasElegidas, posicion);
		nodoSistema* nodo = list_find(nodosSistema, (void*)esNodo);
		nodo->wl += 1;
	}
}

datosBalanceo* buscarBloque(t_list* listaDeBalanceo, infoDeFs* bloque, int posicionIncial){
	datosBalanceo* nodo;
	uint32_t posicionActual = posicionIncial + 1;
	while(1){
		//Si doy toda la vuelta y no encontre el bloque, incremento la availability de todos
		if(posicionActual == posicionIncial){
			incrementarAvailability(listaDeBalanceo);
		}
		//Si llegue al maximo, vuelvo a empezar
		if(posicionActual >= list_size(listaDeBalanceo)){
			posicionActual = 0;
		}
		nodo = list_get(listaDeBalanceo, posicionIncial);
		if(tieneAvailability(nodo) && tieneBloqueBuscado(nodo, bloque)){
			break;
		}else{
			posicionActual++;
		}
	}
	return nodo;
}

//esto esta re sherovi, preguntar
bool laTieneOtroNodo(infoDeFs* bloqueABuscar, t_list* listaDeBalanceo){
	bool esElBloque(int nroBloque){
		return nroBloque == bloqueABuscar->nroBloque;
	}
	bool tieneBloqueYEstaDisponible(datosBalanceo* nodo){
		return list_any_satisfy(nodo->bloques, (void*)esElBloque) && nodo->availability>0;
	}
	return list_any_satisfy(listaDeBalanceo, (void*)tieneBloqueYEstaDisponible);
}

copia* obtenerCopia(datosBalanceo* nodo, infoDeFs* bloque){
	if(!strcmp(nodo->nombreNodo, bloque->copia1->nombreNodo)){
		return bloque->copia1;
	}else if(!strcmp(nodo->nombreNodo, bloque->copia2->nombreNodo)){
		return bloque->copia2;
	}else{
		log_error(loggerYAMA, "Error al obtener los datos de la copia a utilizar.");
		exit(-1);
	}
}
bool tieneBloqueBuscado(datosBalanceo* nodo, infoDeFs* bloque){
	bool esElBloque(int nroBloque){
		return nroBloque == bloque->nroBloque;
	}
	return list_any_satisfy(nodo->bloques, (void*)esElBloque);
}

bool tieneAvailability(datosBalanceo* nodo){
	return nodo->availability > 0;
}

void incrementarAvailabilityDeNodo(datosBalanceo* nodo){
	nodo->availability++;
}

t_list* balancearTransformacion(t_list* listaDeBloques, t_list* listaDeBalanceo){
	int posicion = 0, cantidadAsignados= 0; //como la lista esta ordenada por availability, la availability mas alta es la del primero
	t_list* copiasElegidas = list_create();
	datosBalanceo* nodoAuxiliar = NULL;
	while(cantidadAsignados < list_size(listaDeBloques)){
		infoDeFs* bloqueABuscar = list_get(listaDeBloques, cantidadAsignados);
		//si ya pegue la vuelta, vuelvo a empezar
		if(posicion >= list_size(listaDeBalanceo)){
			posicion = 0;
		}
		//chequeo si el que viene es igual al que tuvo el bloque en el recorrido auxiliar
		if(list_get(listaDeBalanceo, posicion) == nodoAuxiliar){
			incrementarAvailabilityDeNodo(nodoAuxiliar);
			posicion++;
		}
		datosBalanceo* nodoAChequear = list_get(listaDeBalanceo, posicion);
		if(tieneAvailability(nodoAChequear)){
			if(tieneBloqueBuscado(nodoAChequear, bloqueABuscar)){
				posicion++;
				copia* copiaElegida = obtenerCopia(nodoAChequear, bloqueABuscar);
				list_add(copiasElegidas, copiaElegida);
				log_info(loggerYAMA, "El nodo %s fue elegido para transformar el bloque %d.", copiaElegida->nombreNodo, bloqueABuscar->nroBloque);
				cantidadAsignados++;
				//paso a buscar el bloque con un puntero auxiliar
			}else if(laTieneOtroNodo(bloqueABuscar, listaDeBalanceo)){
				nodoAuxiliar = buscarBloque(listaDeBalanceo, bloqueABuscar, posicion+1);
				list_add(copiasElegidas, obtenerCopia(nodoAuxiliar, bloqueABuscar));
				cantidadAsignados++;
			}else{
				log_error(loggerYAMA, "Ocurrio un error en el algoritmo de balanceo de transformacion - Cerrando YAMA.");
				exit(-1);
			}
		}else{
			posicion++;
		}
		actualizarWLTransformacion(copiasElegidas);
	}
	return copiasElegidas;
}

//ACTUALIZO WL EN REDUCCION LOCAL

void actualizarWLRLocal(char* nombreNodo, int cantTemporales){
	bool esNodo(nodoSistema* nodo){
		return !strcmp(nodo->nombreNodo, nombreNodo);
	}
	nodoSistema* nodo = list_find(nodosSistema, (void*)esNodo);
	nodo->wl += cantTemporales;
//	nodo->wl += cantTemporales/2;
//	if(cantTemporales%2 != 0){
//		nodo->wl++;
//	}
}

//BALANCEO LA REDUCCION GLOBAL

char* balancearReduccionGlobal(t_list* listaDeBalanceo){
	int minimoWL = -1;
	uint32_t posicion;
	datosBalanceo* nodoBalanceado;
	nodoSistema* nodoElegido = generarNodoSistema();
	bool esNodo(nodoSistema* nodo){
		return strcmp(nodo->nombreNodo, nodoBalanceado->nombreNodo);
	}
	for(posicion = 0; posicion < list_size(listaDeBalanceo); posicion++){
		nodoBalanceado = list_get(listaDeBalanceo, posicion);
		nodoSistema* nodoAChequear = list_find(nodosSistema, (void*)esNodo);
		if(nodoAChequear->wl < minimoWL || minimoWL == -1){
			minimoWL = nodoAChequear->wl;
			nodoElegido->nombreNodo = nodoAChequear->nombreNodo;
			nodoElegido->wl = nodoAChequear->wl;
		}
	}
	return nodoElegido->nombreNodo;
}


void eliminarTrabajosLocales(t_list* listaTransformaciones){
  uint32_t posicion;
  for(posicion = 0; posicion < list_size(listaTransformaciones); posicion++){
    administracionYAMA * admin = list_get(listaTransformaciones, posicion);
    //UNA POR TRANSFORMACION
    reducirWL(admin->nombreNodo);
    //UNA POR REDUCCION LOCAL
    reducirWL(admin->nombreNodo);
  }
}

void eliminarTrabajosGlobales(int nroMaster, t_list* listaReducLocales){
  char* nodoEncargado = buscarNodoEncargado(nroMaster);
  uint32_t cantidadWLAReducir = list_size(listaReducLocales)/2;
  if(cantidadWLAReducir%2 != 0){
	  cantidadWLAReducir++;
  }
  int i;
  for(i = 0; i < cantidadWLAReducir; i++){
    reducirWL(nodoEncargado);
  }
}

void actualizarWLRGlobal(char* nombreNodo, int cantidadReducs){
	bool esNodo(nodoSistema* nodo){
		return strcmp(nodo->nombreNodo, nombreNodo);
	}
	nodoSistema* nodo = list_find(nodosSistema, (void*)esNodo);
	nodo->wl += cantidadReducs/2;
	if(cantidadReducs % 2 != 0){
		nodo->wl++;
	}
}
