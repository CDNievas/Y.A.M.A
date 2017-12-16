/*
 * nodos.c
 *
 *  Created on: 4/12/2017
 *      Author: utnso
 */

#include "nodos.h"

void actualizoBitmapsNodosDisponibles(){
	int cont=0;
	int cantidadNodos=list_size(listaBitmaps);
	while(cont<cantidadNodos){
		strBitmaps* nodoBitmapSeleccionado=list_get(listaBitmaps,cont);

		bool esElNodo(strNodo* nodoSeleccionado){
			return (strcmp(nodoSeleccionado->nombre,nodoBitmapSeleccionado->nodo)==0);
		}
		strNodo* nodo=list_find(tablaNodos->nodos,(void*)esElNodo);

		int contador=0;
		for(;contador < nodo->tamanioTotal;contador++){
			bitarray_clean_bit(nodoBitmapSeleccionado->bitarray,contador);
		}

		nodo->tamanioLibre=nodo->tamanioTotal;
		nodo->porcentajeOscioso=(nodo->tamanioLibre*100)/nodo->tamanioTotal;
		cont++;
	}
}


void limpiarNodosDesonectados(){
	int contador=0;
	bool nodoDesconectado(strNodo* nodoElegido){
		return(nodoElegido->conectado==false);
	}
	int cantidadNodos=list_count_satisfying(tablaNodos->nodos,(void*)nodoDesconectado);

	while(contador<cantidadNodos){
		//BORRO DE LA TABLA DE NODOS
		strNodo* nodoSeleccionado=list_remove_by_condition(tablaNodos->nodos,(void*)nodoDesconectado);

		//ACTUALIZO LA CAPACIDAD DEL FS
		tablaNodos->tamanioFSLibre-=nodoSeleccionado->tamanioLibre;
		tablaNodos->tamanioFSTotal-=nodoSeleccionado->tamanioTotal;

		//BORRO DE LA LISTA DE NODOS
		bool nodoVictima(char* nombreNodoElegido){
			return(strcmp(nombreNodoElegido,nodoSeleccionado->nombre)==0);
		}
		char* nombreNodo=list_remove_by_condition(tablaNodos->listaNodos,(void*)nodoVictima);

		//BORRO LA ESTRCUTURA DE BITMAPS
		bool liberarBitmap(strBitmaps* nodoBitmapSeleccionado){
			return(strcmp(nodoBitmapSeleccionado->nodo,nombreNodo)==0);
		}
		strBitmaps* nodoBitmap=list_remove_by_condition(listaBitmaps,(void*)liberarBitmap);
		//TENGO QUE BORRAR EL ARCHIVO
		free(nodoBitmap->nodo);
		bitarray_destroy(nodoBitmap->bitarray);
		free(nodoBitmap);

			free(nombreNodo);
		//free(nodoSeleccionado->nombre); ES NOMBRE NODO
		free(nodoSeleccionado);
		contador++;
	}

}

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
			printf("El archivo %s ya cuenta con una copia por bloque.",entradaArchivos->nombre);
			log_debug(loggerFileSystem,"El archivo %s ya cuenta con una copia por bloque.",entradaArchivos->nombre);
		}
		if(hayUnEstadoEstable()==true){
			estadoEstable=true;
			printf("El sistema ya se encuentra en un estado estable.");
			log_debug(loggerFileSystem,"El sistema ya se encuentra en un estado estable.");
		}
	}
 }

int cantidadDeNodosDisponibles(){
	int contador=0;
	int cont=0;
	int cantidadNodos=list_size(tablaNodos->nodos);
	while(cont<cantidadNodos){
		strNodo* nodoSeleccionado=list_get(tablaNodos->nodos,contador);
		if(nodoSeleccionado->conectado==true){
			contador++;
		}
		cont++;
	}
	return contador;
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

		strConexiones* conexionesNodo=malloc(sizeof(strConexiones));
		conexionesNodo->nodo=string_new();
		conexionesNodo->ip=string_new();
		string_append(&conexionesNodo->nodo,nombreNodo);
		string_append(&conexionesNodo->ip,ip);
		conexionesNodo->puerto=puerto;
		list_add(listaConexionesNodos,conexionesNodo);


		bool todosNodosDisponibles(strNodo* entradaNodo){
			return(entradaNodo->conectado==true);
		}

		if(list_all_satisfy(tablaNodos->nodos,(void*)todosNodosDisponibles)){
			printf("Se han conectado todo los nodos del sistemaAnterior");
			log_debug(loggerFileSystem,"Se han conectado todo los nodos del sistemaAnterior");
		}
		verificarCopiasNodo(nombreNodo);

	}else{
		printf("El nodo que se intento conectar no estaba registrado en el estado anterior del sistema.");
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
		printf("%s desconectado registrado nuevamente al sistema.",nombreNodo);
		log_debug(loggerFileSystem,"%s desconectado registrado nuevamente al sistema.",nombreNodo);

		bool todosNodosDisponibles(strNodo* entradaNodo){
			return(entradaNodo->conectado==true);
		}

		if(list_all_satisfy(tablaNodos->nodos,(void*)todosNodosDisponibles)){
			seDesconectoUnNodo=false;
			printf("Todos los nodos del sistema se han conectado.");
			log_debug(loggerFileSystem,"Todos los nodos del sistema se han conectado.");
		}

	}else{
		printf("El nodo que se intento conectar no estaba registrado en el estado anterior del sistema.");
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
		if(sistemaFormateado==false){
			t_bitarray* bitarray = crearBitmap(cantBloques);

			// Creo el nuevo nodo
			strNodo * nuevoNodo = malloc(sizeof(strNodo));
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

		}else{
			if((seDesconectoUnNodo && estadoAnterior)||(seDesconectoUnNodo&&sistemaFormateado)){
				verificarSiEsUnNodoDesconectado(nombreNodo,socket,ip,puerto);
			}else{
				printf("No se puede registrar el Datanode al sistema, ya ha sido formateado");
				log_error(loggerFileSystem,"No se puede registrar el Datanode al sistema, ya ha sido formateado");
				//close(socket);
				sendDeNotificacion(socket, 0);
			}
		}

	}
	free(nombreNodo);
	free(ip);

}



bool asignarEnviarANodo(void* contenidoAEnviar, uint32_t tamanioContenido, strBloqueArchivo* copiasBloque){

	//ARMO EL MENSAJE QUE LE VOY A ENVIAR A DATANODE
	void* mensaje=malloc(sizeof(uint32_t)*3+tamanioContenido);
	uint32_t posicionActualDelMensaje=0;

	//FILTRO LA LISTA GLOBAL DE NODOS POR SI ESTAN DISPONIBLES Y SI TIENEN ESPACIO LIBRE
	bool estaDisponible(strNodo* nodoSeleccionado){
		return (nodoSeleccionado->conectado==true && nodoSeleccionado->porcentajeOscioso>0);
	}
	t_list* listaNodosDisponiblesEnElSistema=list_filter(tablaNodos->nodos,(void*)estaDisponible);

	//ORDENO LA LISTA FILTRADA POR EL POCENTAJE QUE TENGA OCIOSO
	bool ordenarPorPorcentajeOcioso(strNodo* nodoSeleccionado1, strNodo* nodoSeleccionado2){
		return(nodoSeleccionado1->porcentajeOscioso > nodoSeleccionado2->porcentajeOscioso);
	}
	list_sort(listaNodosDisponiblesEnElSistema,(void*)ordenarPorPorcentajeOcioso);

	//ELiJO EL NODO QUE TENGA EL ORIGINAL
	strNodo* nodoOriginal=list_get(listaNodosDisponiblesEnElSistema,0);

	if(nodoOriginal==NULL){
		log_error(loggerFileSystem,"No se pudo asignar un nodo para el bloque original del archivo. No hay nodos suficientes.");
		exit (-1);
	}
		//MODIFICO LOS DATOS DEL NODO
	nodoOriginal->tamanioLibre--;
	nodoOriginal->porcentajeOscioso=(nodoOriginal->tamanioLibre*100)/nodoOriginal->tamanioTotal;
	//DISMINUYO LA CANTIDAD DE BLOQUES LIBRES DEL FS
	tablaNodos->tamanioFSLibre--;

		//ASIGNO UN BLOQUE LIBRE AL NODO
	uint32_t bloqueAsignado=asignarBloqueNodo(nodoOriginal);

	//ENVIO EL ORIGINAL
	copiasBloque->copia1=malloc(sizeof(strCopiaArchivo));
	copiasBloque->copia1->nroBloque=bloqueAsignado;
	copiasBloque->copia1->nodo=string_new();

	log_info(loggerFileSystem,"Bloque original: %s | %d",nodoOriginal->nombre,bloqueAsignado);

	string_append(&copiasBloque->copia1->nodo,nodoOriginal->nombre);

	memcpy(mensaje,&bloqueAsignado,sizeof(uint32_t));
	posicionActualDelMensaje+=sizeof(uint32_t);

	memcpy(mensaje+posicionActualDelMensaje,&tamanioContenido,sizeof(uint32_t));
	posicionActualDelMensaje+=sizeof(uint32_t);

	memcpy(mensaje+posicionActualDelMensaje,&tamanioContenido,sizeof(uint32_t));
	posicionActualDelMensaje+=sizeof(uint32_t);

	memcpy(mensaje+posicionActualDelMensaje,contenidoAEnviar,tamanioContenido);
	posicionActualDelMensaje+=tamanioContenido;

	if(FD_ISSET(nodoOriginal->socket,&socketClientesAuxiliares)){
		FD_CLR(nodoOriginal->socket,&socketClientesAuxiliares);
	}
	
	sendRemasterizado(nodoOriginal->socket,ENV_ESCRIBIR,posicionActualDelMensaje,mensaje);

	if(recvDeNotificacion(nodoOriginal->socket)==ESC_INCORRECTA){
		log_error(loggerFileSystem,"No se pudo almacenar el archivo en el %d", nodoOriginal->nombre);
		return false;
	}

	//ELIJO EL NODO QUE TIENE LA COPIA
	strNodo* nodoCopia=list_get(listaNodosDisponiblesEnElSistema,1);
	if(nodoCopia==NULL){
		nodoCopia=list_get(listaNodosDisponiblesEnElSistema,0);
		log_error(loggerFileSystem,"Por falta de nodos para almacenar la copia, se procede a guardar la copia en el mismo lugar que el original.");
	}
		//MODIFICO LOS DATOS DEL NODO
	nodoCopia->tamanioLibre--;
	nodoCopia->porcentajeOscioso=(nodoCopia->tamanioLibre*100)/nodoCopia->tamanioTotal;
	//DISMINUYO LA CANTIDAD DE BLOQUES LIBRES DEL FS
	tablaNodos->tamanioFSLibre--;

	bloqueAsignado=asignarBloqueNodo(nodoCopia);

	//ENVIO LA COPIA
	mensaje=realloc(mensaje,sizeof(uint32_t)*3+tamanioContenido);

	posicionActualDelMensaje=0;

	copiasBloque->copia2=malloc(sizeof(strCopiaArchivo));
	copiasBloque->copia2->nroBloque=bloqueAsignado;
	copiasBloque->copia2->nodo=string_new();

	log_info(loggerFileSystem,"Bloque original: %s | %d",nodoCopia->nombre,bloqueAsignado);

	string_append(&copiasBloque->copia2->nodo,nodoCopia->nombre);

	memcpy(mensaje,&bloqueAsignado,sizeof(uint32_t));
	posicionActualDelMensaje+=sizeof(uint32_t);

	memcpy(mensaje+posicionActualDelMensaje,&tamanioContenido,sizeof(uint32_t));
	posicionActualDelMensaje+=sizeof(uint32_t);

	memcpy(mensaje+posicionActualDelMensaje,&tamanioContenido,sizeof(uint32_t));
	posicionActualDelMensaje+=sizeof(uint32_t);

	memcpy(mensaje+posicionActualDelMensaje,contenidoAEnviar,tamanioContenido);
	posicionActualDelMensaje+=tamanioContenido;

	if(FD_ISSET(nodoCopia->socket,&socketClientesAuxiliares)){
		FD_CLR(nodoCopia->socket,&socketClientesAuxiliares);
	}
	   
	sendRemasterizado(nodoCopia->socket,ENV_ESCRIBIR,posicionActualDelMensaje,mensaje);

	if(recvDeNotificacion(nodoCopia->socket)==ESC_INCORRECTA){
		log_error(loggerFileSystem,"No se pudo almacenar el archivo en el %d", nodoCopia->nombre);
		return false;
	}

	free(mensaje);
	list_destroy(listaNodosDisponiblesEnElSistema);
	return true;


}


void enviarDatosANodo(t_list* posicionesBloquesAGuardar,FILE* archivoALeer,strArchivo* entradaArchivoAGuardar){
	uint32_t bloqueActual=0;

	void enviarInfoNodoPorPosicion(uint32_t posicion){
		//GENERO LAS INSTANCIAS DE LOS BLOQUES DEL ARCHIVO
		strBloqueArchivo* copiasBloque=malloc(sizeof(strBloqueArchivo));
		copiasBloque->nro=bloqueActual;

		if(bloqueActual==0){
			void* contenidoAEnviar=malloc(posicion);
			fread(contenidoAEnviar,posicion,1,archivoALeer);
			//ASIGNO LOS BYTES QUE OCUPA EL BLOQUE
			copiasBloque->bytes=posicion;
			log_info(loggerFileSystem,"Bloque: %d | Bytes: %d",bloqueActual,posicion);
			//ASIGNO LOS NODOS A DONDO QUIERO GUARDAR EL CONTENIDO
			if(asignarEnviarANodo(contenidoAEnviar,posicion,copiasBloque)==false){
				return;
			}
			free(contenidoAEnviar);
		}else{
			uint32_t posicionAnterior = (uint32_t) list_get(posicionesBloquesAGuardar,bloqueActual-1);
			//DETERMINO LA CANTIDAD EXACTA QUE TENGO QUE GUARDAR DEL BLOQUE
			uint32_t tamanioALeer=posicion-posicionAnterior;
			void* contenidoAEnviar=malloc(tamanioALeer);
			fread(contenidoAEnviar,tamanioALeer,1,archivoALeer);
			//ASIGNO LOS BYTES QUE OCUPA EL BLOQUE
			copiasBloque->bytes=tamanioALeer;
			log_info(loggerFileSystem,"Bloque: %d | Bytes: %d",bloqueActual,tamanioALeer);
			//ASIGNO LOS NODOS DONDE QUIERO GUARDAR EL CONTENIDO
			if(asignarEnviarANodo(contenidoAEnviar,tamanioALeer,copiasBloque)==false){
				return;
			}
			free(contenidoAEnviar);
		}
		bloqueActual++;
		list_add(entradaArchivoAGuardar->bloques,copiasBloque);
	}
	list_iterate(posicionesBloquesAGuardar,(void*) enviarInfoNodoPorPosicion);
}


int asignarBloqueNodo(strNodo* nodoOriginal){
	int posicionEnELBitarray=0;
	bool esElNodo(strBitmaps* BitmapNodo){
		return (strcmp(nodoOriginal->nombre,BitmapNodo->nodo)==0);
	}
	strBitmaps* BitarrayNodo = list_find(listaBitmaps,(void*)esElNodo);
	for(;posicionEnELBitarray < nodoOriginal->tamanioTotal; posicionEnELBitarray++){
		if(!bitarray_test_bit(BitarrayNodo->bitarray,posicionEnELBitarray)){
			bitarray_set_bit(BitarrayNodo->bitarray,posicionEnELBitarray);
			return posicionEnELBitarray;
		}
	}
	return 0;//TENGO QUE CACHEAR ESTE ERROR?
}

void mostrarEstadoDelSistemaNodos(){
	int cont=0;
	int cantidadNodos=list_size(tablaNodos->nodos);
	printf("Mostrando informacion del sistema:\n");
	log_info(loggerFileSystem,"Mostrando informacion del sistema:\n");
	while(cont<cantidadNodos){
		strNodo* nodoSeleccionado=list_get(tablaNodos->nodos,cont);
		printf("-%s: \n",nodoSeleccionado->nombre);
		log_info(loggerFileSystem,"-%s: \n",nodoSeleccionado->nombre);
		printf("Tamaño total: %d | Tamaño libre: %d | Porcentaje ocioso: %d\n",nodoSeleccionado->tamanioTotal,nodoSeleccionado->tamanioLibre,nodoSeleccionado->porcentajeOscioso);
		log_info(loggerFileSystem,"Tamaño total: %d | Tamaño libre: %d | Porcentaje ocioso: %d\n",nodoSeleccionado->tamanioTotal,nodoSeleccionado->tamanioLibre,nodoSeleccionado->porcentajeOscioso);
		cont++;
	}
}
