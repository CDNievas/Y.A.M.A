/*
 * Nodos.c
 *
 *  Created on: 25/11/2017
 *      Author: utnso
 */

#include "Nodos.h"
#include "../../Biblioteca/src/Socket.h"

uint32_t buscarIndexEnLaTablaGlobalNodos(contenidoNodo* nodoDesconectado){
	uint32_t posicion=0;
	uint32_t cantidadDeNodos=list_size(tablaGlobalNodos);
	while(posicion<cantidadDeNodos){
		contenidoNodo* nodoSeleccionado=list_get(tablaGlobalNodos->contenidoXNodo,posicion);
		if(strcmp(nodoSeleccionado->nodo,nodoDesconectado->nodo)==0){
			return posicion;
		}
		posicion++;
	}
	return -1;

}


void verificarQueNodo(int socket){
	bool esElNodoDesconectado(contenidoNodo* nodoSeleccionado){
		return (nodoSeleccionado->socket==socket);
	}
	contenidoNodo* nodoDesconectado=list_find(tablaGlobalNodos->contenidoXNodo,(void*)esElNodoDesconectado);

	if(nodoDesconectado!=NULL){
		log_error(loggerFileSystem,"Se ha desconectado un DataNode del sistema.");
		tablaGlobalNodos->tamanio-=nodoDesconectado->total;
		tablaGlobalNodos->libres-=nodoDesconectado->libre;
		nodoDesconectado->disponible=0;
		seDesconectoUnNodo=true;
	}
}


int cantBloquesLibres(t_bitarray* bitarray, uint32_t tamaniobitarray) {
	int i = 0;
	int cont = 0;
	for (; i < tamaniobitarray; i++) {
		if (!bitarray_test_bit(bitarray, i)) {
			cont++;
		}
	}
	return cont;
}


int sacarPorcentajeOcioso(int bloquesLibres, int cantBloques){
	int numero = (bloquesLibres*100)/cantBloques;
	if((bloquesLibres*100)%cantBloques != 0){
	 numero++;
	}
	return numero;
}

//void borrarNodosRegistradosNoDisponibles(){
//	uint32_t cantidadDeNodos=list_size(tablaGlobalNodos->nodo);
//	uint32_t cont=0;
//	while(cont<cantidadDeNodos){
//
//		bool noEstaDsponible(contenidoNodo* nodoSeleccionado){
//			return (nodoSeleccionado->disponible==0);
//		}
//		contenidoNodo* nodo=list_remove_by_condition(tablaGlobalNodos->contenidoXNodo,(void*)noEstaDsponible);
//
//		if(nodo!=NULL){
//
//			tablaGlobalNodos->tamanio-=nodo->total;
//			tablaGlobalNodos->libres-=nodo->libre;
//
//			free(nodo->nodo);
//			free(nodo);
//
//		}
//
//		cont++;
//
//	}
//	persistirTablaNodo();
//
//}

bool hayUnEstadoEstable(){

	bool todosArchivosDisponibles(tablaArchivos* entradaArchivo){
		return(entradaArchivo->disponible==1);
	}

	if(list_all_satisfy(tablaGlobalArchivos,(void*)todosArchivosDisponibles)){
		return true;
		log_debug(loggerFileSystem,"El sistema ya se encuentra en un estado estable.");
	}else{
		return false;
	}
}

void verificarCopiasNodo(char* nombreNodo){
	uint32_t cantidadDeArchivos=list_size(tablaGlobalArchivos);
	uint32_t cont=0;
	while(cont<cantidadDeArchivos){
		tablaArchivos* entradaArchivos=list_get(tablaGlobalArchivos,cont);

		uint32_t cantidadDeBloques=list_size(entradaArchivos->bloques);
		uint32_t contNodo=0;

		while(contNodo<cantidadDeBloques){
			copiasXBloque* bloqueCopia=list_get(entradaArchivos->bloques,contNodo);

			if((strcmp(bloqueCopia->copia1->nodo,nombreNodo)==0 ||strcmp(bloqueCopia->copia2->nodo,nombreNodo))){
				bloqueCopia->disponible=1;
			}
			contNodo++;
		}
		cont++;

		bool todosBloquesDisponibles(copiasXBloque* entradaBloqueCopia){
			return(entradaBloqueCopia->disponible==1);
		}

		if(list_all_satisfy(entradaArchivos->bloques,(void*)todosBloquesDisponibles)){
			entradaArchivos->disponible=1;
			log_debug(loggerFileSystem,"El archivo %s ya cuenta con una copia por bloque.",nombreNodo);
		}
	}
}




void perteneceAlSistema(char* nombreNodo, int socket, char* ip, uint32_t puerto){

	bool estaEnElSistema(char* nodoSeleccionado){
		return(strcmp(nodoSeleccionado,nombreNodo)==0);
	}
	bool estabaEnElEstadoAnterior=list_any_satisfy(tablaGlobalNodos->nodo,(void*)estaEnElSistema);
	if(estabaEnElEstadoAnterior){
		bool esElNodo(contenidoNodo* nodo){
			return(strcmp(nodo->nodo,nombreNodo)==0);
		}
		contenidoNodo* nodoElegido =list_find(tablaGlobalNodos->contenidoXNodo,(void*)esElNodo);

		nodoElegido->disponible=1;
		nodoElegido->socket=socket;


		bool todosNodosDisponibles(contenidoNodo* entradaNodo){
			return(entradaNodo->disponible==1);
		}

		if(list_all_satisfy(tablaGlobalNodos->contenidoXNodo,(void*)todosNodosDisponibles)){
			hayEstadoAnterior=false;
		}
		verificarCopiasNodo(nombreNodo);

	}else{
		log_error(loggerFileSystem,"El nodo que se intento conectar no estaba registrado en el estado anterior del sistema.");
		sendDeNotificacion(socket,DESCONECTAR_NODO);
	}
}

//uint32_t buscarIndexEnLaListaNodosDesconectados(contenidoNodo* nodoElegido){
//	uint32_t posicion=0;
//	uint32_t cantidadDeNodos=list_size(listaNodosDesconectacdos);
//	while(posicion<cantidadDeNodos){
//		contenidoNodo* nodoSeleccionado=list_get(listaNodosDesconectacdos,posicion);
//		if(strcmp(nodoSeleccionado->nodo,nodoElegido->nodo)==0){
//			return posicion;
//		}
//		posicion++;
//	}
//	return -1;
//
//}


void verificarSiEsNodoDesconectado(char* nombreNodo, uint32_t socket,char* ip,uint32_t puerto){

	 bool estaEnElSistema(char* nodoSeleccionado){
		 return(strcmp(nodoSeleccionado,nombreNodo)==0);
	 }
	 bool estabaEnElEstadoAnterior=list_any_satisfy(tablaGlobalNodos->nodo,(void*)estaEnElSistema);
	 if(estabaEnElEstadoAnterior){
		bool esElNodo(contenidoNodo* nodo){
			return(strcmp(nodo->nodo,nombreNodo)==0);
		}
		contenidoNodo* nodoElegido =list_find(tablaGlobalNodos->contenidoXNodo,(void*)esElNodo);

		nodoElegido->disponible=1;
		nodoElegido->socket=socket;
		tablaGlobalNodos->tamanio+=nodoElegido->total;
		tablaGlobalNodos->libres+=nodoElegido->libre;

		log_debug(loggerFileSystem,"%d desconectado registrado nuevamente al sistema.",nombreNodo);

		bool todosNodosDisponibles(contenidoNodo* entradaNodo){
			return(entradaNodo->disponible==1);
		}

		if(list_all_satisfy(tablaGlobalNodos->contenidoXNodo,(void*)todosNodosDisponibles)){
			seDesconectoUnNodo=false;
			log_debug(loggerFileSystem,"Todos los nodos del sistema se han conectado.");
		}

	}else{
		log_error(loggerFileSystem,"El nodo que se intento conectar no estaba registrado en el estado anterior del sistema.");
		sendDeNotificacion(socket,DESCONECTAR_NODO);
	}
}

void registrarNodo(int socket) {

	uint32_t cantBloques,puerto;
	t_bitarray * bitarray;

	char * nombreNodo = recibirString(socket);
	cantBloques = recibirUInt(socket);
	char * ip = recibirString(socket);

	log_debug(loggerFileSystem,"Se ha registrado el %s con %d bloques libres.",nombreNodo,cantBloques);
	puerto=recibirUInt(socket);

	// Checkeo estado anterior
	if(hayEstadoAnterior==true){
		perteneceAlSistema(nombreNodo,socket,ip,puerto);
	} else {
		if(estaFormateado==false && seDesconectoUnNodo==false){
			bitarray = crearBitmap(nombreNodo,cantBloques);

			// Creo el bloque de info del nodo
			contenidoNodo* bloque = malloc(sizeof(contenidoNodo));
			bloque->nodo = string_new();
			string_append(&bloque->nodo, nombreNodo);
			bloque->disponible=1;
			bloque->total = cantBloques;
			bloque->libre = cantBloquesLibres(bitarray,cantBloques);
			bloque->porcentajeOcioso=sacarPorcentajeOcioso(bloque->libre,cantBloques);
			bloque->socket=socket;

			// Aniado a la tabla de info de nodos
			tablaGlobalNodos->tamanio += bloque->total;
			tablaGlobalNodos->libres += bloque->libre;
			list_add(tablaGlobalNodos->nodo, nombreNodo);
			list_add(tablaGlobalNodos->contenidoXNodo, bloque);

			// Aniado a la tala de bitmaps
			tablaBitmapXNodos* bitmapNodo = malloc(sizeof(tablaBitmapXNodos));
			bitmapNodo->nodo=string_new();
			string_append(&bitmapNodo->nodo, nombreNodo);
			bitmapNodo->bitarray = bitarray;
			list_add(listaBitmap, bitmapNodo);

			persistirTablaNodo();//Aniado los datos de conexion del nodo

			datosConexionNodo* datosConexion=malloc(sizeof(datosConexionNodo));
			datosConexion->nodo=string_new();
			string_append(&datosConexion->nodo, nombreNodo);
			datosConexion->ip=string_new();
			string_append(&datosConexion->ip, ip);
			datosConexion->puerto=puerto;
			list_add(listaConexionNodos,datosConexion);
			log_debug(loggerFileSystem,"Nodo registrado correctamente.");
		} else{
			if(seDesconectoUnNodo==true){
						verificarSiEsNodoDesconectado(nombreNodo,socket,ip,puerto);
			}else{
				log_error(loggerFileSystem,"No se ha podido registrar el nodo. Ya ha sido formateado el sistema");
				sendDeNotificacion(socket,DESCONECTAR_NODO);
			}
		}
	}


}




