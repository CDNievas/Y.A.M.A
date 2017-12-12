/*
 * nodos.c
 *
 *  Created on: 4/12/2017
 *      Author: utnso
 */

#include "nodos.h"

void verificarCopiasNodo(char* nombreNodo){
	uint32_t cantidadDeArchivos=list_size(tablaArchivos);
	uint32_t cont=0;
	while(cont<cantidadDeArchivos){
		strArchivo* entradaArchivos=list_get(tablaArchivos,cont);

		uint32_t cantidadDeBloques=list_size(entradaArchivos->bloques);
		uint32_t contNodo=0;

		while(contNodo<cantidadDeBloques){
			strBloqueArchivo* bloqueCopia=list_get(entradaArchivos->bloques,contNodo);

			if(strcmp(bloqueCopia->copia1->nodo,nombreNodo)==0 || strcmp(bloqueCopia->copia2->nodo,nombreNodo)==0){
				bloqueCopia->disponible=true;
			}
			contNodo++;
		}
		cont++;

		bool todosBloquesDisponibles(strBloqueArchivo* entradaBloqueCopia){
			return(entradaBloqueCopia->disponible==true);
		}

		if(list_all_satisfy(entradaArchivos->bloques,(void*)todosBloquesDisponibles)){
			entradaArchivos->disponible=true;
			log_debug(loggerFileSystem,"El archivo %s ya cuenta con una copia por bloque.",entradaArchivos->nombre);
		}
	}
 }

void perteneceAlSistema(char* nombreNodo, int socket, char* ip, uint32_t puerto){

	bool estaEnElSistema(char* nodoSeleccionado){
		return(strcmp(nodoSeleccionado,nombreNodo)==0);
	}
	bool estabaEnElEstadoAnterior=list_any_satisfy(tablaNodos->listaNodos,(void*)estaEnElSistema);
	if(estabaEnElEstadoAnterior){
		bool esElNodo(strNodo* nodo){
			return(strcmp(nodo->nombre,nombreNodo)==0);
		}
		strNodo* nodoElegido =list_find(tablaNodos->nodos,(void*)esElNodo);

		nodoElegido->conectado=true;
		nodoElegido->socket=socket;


		bool todosNodosDisponibles(strNodo* entradaNodo){
			return(entradaNodo->conectado==true);
		}

		if(list_all_satisfy(tablaNodos->nodos,(void*)todosNodosDisponibles)){
			estadoAnterior=false;
		}
		verificarCopiasNodo(nombreNodo);

	}else{
		log_error(loggerFileSystem,"El nodo que se intento conectar no estaba registrado en el estado anterior del sistema.");
		sendDeNotificacion(socket,DESCONECTAR_NODO);
	}
}


bool hayUnEstadoEstable(){

	bool todosArchivosDisponibles(strArchivo* entradaArchivo){
		return(entradaArchivo->disponible==true);
	}

	if(list_all_satisfy(tablaArchivos,(void*)todosArchivosDisponibles)){
		return true;
		log_debug(loggerFileSystem,"El sistema ya se encuentra en un estado estable.");
	}else{
		return false;
	}
}

void registrarNodosConectados(){

	void pidoInfo(int * socket){
		sendDeNotificacion((* socket),PEDIR_INFONODO);
	}

	list_iterate(socketsDatanode,(void *)pidoInfo);
	list_destroy(socketsDatanode);


}

void verificarSiEsUnNodoDesconectado(char* nombreNodo, uint32_t socket,char* ip,uint32_t puerto){

	 bool estaEnElSistema(char* nodoSeleccionado){
		 return(strcmp(nodoSeleccionado,nombreNodo)==0);
	 }
	 bool estabaEnElEstadoAnterior=list_any_satisfy(tablaNodos->listaNodos,(void*)estaEnElSistema);
	 if(estabaEnElEstadoAnterior){
		bool esElNodo(strNodo* nodo){
			return(strcmp(nodo->nombre,nombreNodo)==0);
		}
		strNodo* nodoElegido =list_find(tablaNodos->nodos,(void*)esElNodo);

		nodoElegido->conectado=true;
		nodoElegido->socket=socket;
		tablaNodos->tamanioFSTotal+=nodoElegido->tamanioTotal;
		tablaNodos->tamanioFSLibre+=nodoElegido->tamanioLibre;

		//PERSISTO LA TABLA
		persistirTablaNodo();

		log_debug(loggerFileSystem,"%d desconectado registrado nuevamente al sistema.",nombreNodo);

		bool todosNodosDisponibles(strNodo* entradaNodo){
			return(entradaNodo->conectado==true);
		}

		if(list_all_satisfy(tablaNodos->nodos,(void*)todosNodosDisponibles)){
			seDesconectoUnNodo=false;
			log_debug(loggerFileSystem,"Todos los nodos del sistema se han conectado.");
		}

	}else{
		log_error(loggerFileSystem,"El nodo que se intento conectar no estaba registrado en el estado anterior del sistema.");
		sendDeNotificacion(socket,DESCONECTAR_NODO);
	}
}



void registrarNodo(int socket){

	char * nombreNodo = recibirString(socket);
	uint32_t cantBloques = recibirUInt(socket);
	char * ip = recibirString(socket);
	uint32_t puerto=recibirUInt(socket);

	if(estadoAnterior==true){
		perteneceAlSistema(nombreNodo,socket,ip,puerto);
	}else{
		if(sistemaFormateado==false && seDesconectoUnNodo==false){
			t_bitarray* bitarray = crearBitmap(cantBloques);

			// Creo el nuevo nodo
			strNodo * nuevoNodo = malloc(sizeof(strNodo));

//			int * socketNro = malloc(sizeof(int));
//			(* socketNro) = socket;
//			nuevoNodo->socket=(* socketNro);
			nuevoNodo->socket=socket;
			nuevoNodo->conectado=true;
			nuevoNodo->nombre=string_new();
			string_append(&nuevoNodo->nombre,nombreNodo);
			nuevoNodo->tamanioTotal=cantBloques;
			nuevoNodo->tamanioLibre=cantBloques;
			nuevoNodo->porcentajeOscioso=(nuevoNodo->tamanioLibre*100)/nuevoNodo->tamanioTotal;

			list_add(tablaNodos->nodos,nuevoNodo);

			//AGREGO EL NOMBRE DEL NODO A LA LISTA
			list_add(tablaNodos->listaNodos,nuevoNodo->nombre);

			// Actualizo tamanio del FS
			tablaNodos->tamanioFSTotal+=nuevoNodo->tamanioTotal;
			tablaNodos->tamanioFSLibre+=nuevoNodo->tamanioLibre;

			//AGREGO LA INSTACION EN TABLA BITMAP
			strBitmaps* bitmapNodo=malloc(sizeof(strBitmaps));
			bitmapNodo->nodo=string_new();
			string_append(&bitmapNodo->nodo,nombreNodo);
			bitmapNodo->bitarray=bitarray;
			list_add(listaBitmaps,bitmapNodo);

			//AGREGO LA INSTACIA EN TABLA DE CONEXIONES DE NODO
			strConexiones* conexionesNodo=malloc(sizeof(strConexiones));
			conexionesNodo->nodo=string_new();
			conexionesNodo->ip=string_new();
			string_append(&conexionesNodo->nodo,nombreNodo);
			string_append(&conexionesNodo->ip,ip);
			conexionesNodo->puerto=puerto;
			list_add(listaConexionesNodos,conexionesNodo);


			//PERSISTO LA TABLA
			persistirTablaNodo();

		}else{
			if(seDesconectoUnNodo){
				verificarSiEsUnNodoDesconectado(nombreNodo,socket,ip,puerto);
			}else{
				log_error(loggerFileSystem,"No se puede registrar el Datanode al sistema, ya ha sido formateado");
			}
		}

	}
	free(nombreNodo);
	free(ip);

	/*bool buscaEnlista(strNodo * unNodo){
		return(strcmp(unNodo->nombre,nombreNodo)==0);
	}*/


	/*
	if(sistemaFormateado){

		log_warning(loggerFileSystem,"El sistema ya se encuentra formateado. Cerrando conexion con el nodo");
		sendDeNotificacion(socket,DESCONECTAR_NODO);
		FD_CLR(socket, &socketClientes);
		close(socket);

	} else {

		if(estadoAnterior){

			nodo = list_find(tablaNodos->nodos,(void*)buscaEnlista);

			// Nodo existia en estado anterior
			if(nodo !=NULL){

				nodo->conectado=1;
				nodo->socket=socket;

			// Nuevo nodo
			} else {

				log_warning(loggerFileSystem,"El nodo que se intento conectar no estaba registrado en el estado anterior del sistema. Cerrando conexion con el nodo");
				sendDeNotificacion(socket,DESCONECTAR_NODO);
				FD_CLR(socket, &socketClientes);
				close(socket);

			}


		} else {

			// Aniado nodo a la lista

		}

	}
	*/

}

void simulaBalanceo(int cantBloques){

	int i=0;
	while(i < cantBloques){

		char * bloqueA = string_from_format("A%d",i);
		char * bloqueB = string_from_format("B%d",i);

		i++;

	}
	// Copia bloque
	cantBloques = cantBloques * 2;



}
