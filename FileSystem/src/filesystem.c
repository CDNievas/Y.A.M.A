#include "consolaFS.h"
#include "structFS.h"
#include <math.h>
#include "../../Biblioteca/src/genericas.c"


//---------------------------------------------------BITMAP---------------------------------------------------------

void pedirBloque(int socketNodo, uint32_t nroBloque, uint32_t cantBytes){

	 // PRUEBA LECTURA
	int tamanioMsg = sizeof(uint32_t) + sizeof(uint32_t);
	void * msg = miMalloc(tamanioMsg,loggerFileSystem,"Fallo en pedirBloque()");

	memcpy(msg+sizeof(uint32_t), &nroBloque, sizeof(uint32_t));
	memcpy(msg+sizeof(uint32_t)+sizeof(uint32_t), &cantBytes, sizeof(uint32_t));

	sendRemasterizado(socketNodo,ENV_LEER,tamanioMsg,msg);

	free(msg);

}

void recibirBloque(int socketNodo){

	uint32_t cantBytes = recibirUInt(socketNodo);
	void * bloque = miMalloc(cantBytes,loggerFileSystem,"Fallo en recibirBloque()");
	if(recv(socket, bloque, cantBytes, MSG_WAITALL) == -1){
		perror("Error al recibir el bloque.");
		exit(-1);
	}
	char* bloqueRecibido = string_substring_until(bloque, cantBytes);

	printf("%s",bloqueRecibido);

	free(bloque);

}

char * obtenerPathBitmap(char * nombreNodo){
	char * path = string_new();
	string_append(&path, PATH_METADATA);
	string_append(&path, "bitmap/");
	string_append(&path, nombreNodo);
	string_append(&path, ".dat");
	return path;
}

char * obtenerPathTablaNodo(){
	char * path = string_new();
	string_append(&path, PATH_METADATA);
	string_append(&path, "nodos.bin");
	return path;
}

t_bitarray * abrirBitmap(char * nombreNodo,int cantBloques){

	char * path = obtenerPathBitmap(nombreNodo); // Path bitmap
	int archivo= open(path, O_RDWR); // FD Archivo
	struct stat infoBitmap; // Guarda informacion del archivo
	char * mapArchivo; // Memoria del mmap
	t_bitarray * bitarray; // Bitarray

	if(archivo  < 0){
		// Error al abrir el archivo
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo.");
		exit(-1);
	}

	// Abro el archivo
	if(fstat(archivo,&infoBitmap) < 0){
		// Error al abrir el archivo
		log_error(loggerFileSystem,"Error al tratar de abrir el archivo.");
		exit(-1);
	}



	// Lo mapeo a memoria
	mapArchivo = mmap(0, infoBitmap.st_size, PROT_WRITE | PROT_READ, MAP_SHARED, archivo, 0);
	if (mapArchivo == MAP_FAILED) {
				printf("Error al mapear a memoria: %s\n", strerror(errno));
				close(archivo);
	}

//	int tamanioBitarray=cantBloques/2;
//	 if(cantBloques % 2 != 0){
//	  tamanioBitarray++;
//	 }

	int tamanioBitarray=cantBloques;

	bitarray = bitarray_create_with_mode(mapArchivo,tamanioBitarray,MSB_FIRST);

	if(close(archivo) < 0){
		log_error(loggerFileSystem,"Fallo al cerrar el archivo.");
		exit(-1);
	}
	free(path);
	return bitarray;

}

t_bitarray * crearBitmap(char * nombreNodo, int cantBloques){



	log_debug(loggerFileSystem,"Se procede a crear el archivo Bitmap.bin");
	char * path = obtenerPathBitmap(nombreNodo); // Path bitmap
	int file = open(path, O_RDWR | O_CREAT,S_IRUSR | S_IWUSR);
	char* contenido=string_repeat('\0',cantBloques);
	if(file<0){
		log_info(loggerFileSystem,"No se pudo crear el archivo");
	}
	write(file,contenido,cantBloques);
	free(contenido);
	free(path);
	return abrirBitmap(nombreNodo,cantBloques);


}

//Habria que implementar la copia de los archivos

void cargarFileSystem(t_config* configuracionFS) {
	if (!config_has_property(configuracionFS, "PUERTO_ESCUCHA")) {
		log_error(loggerFileSystem,
				"No se encuentra el parametro PUERTO_ESCUCHA en el archivo de configuracion");
		exit(-1);
	} else {
		PUERTO_ESCUCHA = config_get_int_value(configuracionFS,
				"PUERTO_ESCUCHA");
	}
	if(!config_has_property(configuracionFS, "PATH_METADATA")){
		log_error(loggerFileSystem, "No se encuentra el parametro PATH_METADATA en el archivo de configuracion");
		exit(-1);
	}
	PATH_METADATA = string_new();
	string_append(&PATH_METADATA, config_get_string_value(configuracionFS, "PATH_METADATA"));
	config_destroy(configuracionFS);
}

//---------------------------------Directorios------------------------------
char* obtenerNombreDirectorio(char** rutaDesmembrada){
  int posicion = 0;
  char* nombreArchivo = string_new();
  while(1){
    if(rutaDesmembrada[posicion+1] == NULL){
      string_append(&nombreArchivo, rutaDesmembrada[posicion]);
      break;
    }
    posicion++;
  }
  return nombreArchivo;
}

int obtenerDirectorioPadre(char** rutaDesmembrada){
  char* fathersName = string_new();
  bool isMyFather(t_directory* directory){
    return strcmp(fathersName, directory->nombre) == 0;
  }
  int posicion = 0;
  while(1){
    if(rutaDesmembrada[posicion+1]!=NULL){
      if(rutaDesmembrada[posicion+2] == NULL){
        string_append(&fathersName, rutaDesmembrada[posicion]);
        t_directory* directory = list_find(listaDirectorios, (void*)isMyFather);
        if(directory == NULL){
        	free(fathersName);
          return -1;
        }
        free(fathersName);
        return directory->index;
      }
    }else if(rutaDesmembrada[posicion+1]==NULL){
      return 0;
    }
    posicion++;
  }
}





//--------------------------------Nodos---------------------------------------
int cantBloquesLibres(t_bitarray* bitarray) {
	int i = 0;
	int cont = 0;
	int tamaniobitarray = (bitarray_get_max_bit(bitarray)/8);
	for (; i < tamaniobitarray; i++) {
		if (!bitarray_test_bit(bitarray, i)) {
			cont++;
		}
	}
	return cont;
}

void persistirTablaNodo(){


	//FILE* archivoNodos=fopen("/home/utnso/workspace/tp-2017-2c-ElTPEstaBien/FileSystem/metadata/nodos.bin","r+");
//	char * path = obtenerPathTablaNodo();
	FILE* archivoNodos=fopen("asd.txt","w");

	fputs("TAMANIO=",archivoNodos);
	char* tamanioCadena = string_itoa(tablaGlobalNodos->tamanio);
	fputs(tamanioCadena,archivoNodos);
	free(tamanioCadena);
	fputc('\n',archivoNodos);
	fputs("BLOQUES=[",archivoNodos);
	int i=0;
	while(i<list_size(tablaGlobalNodos->nodo)){
		fputs(list_get(tablaGlobalNodos->nodo,i),archivoNodos);
		fputc(',',archivoNodos);
		i++;
	}
	fputc(']',archivoNodos);
	fputc('\n',archivoNodos);

	int s=0;
	char* nodoSeleccionado;
	while(s<list_size(tablaGlobalNodos->nodo)){
		nodoSeleccionado=list_get(tablaGlobalNodos->nodo,s);
		fputs(nodoSeleccionado,archivoNodos);
		fputs("TOTAL=",archivoNodos);
		bool esNodo(contenidoNodo* contenidoDeUnNodo){
			return(strcmp(contenidoDeUnNodo->nodo,nodoSeleccionado)==0);
		}
		contenidoNodo* nodoElegido=list_find(tablaGlobalNodos->contenidoXNodo ,(void*)esNodo);
		char* totalNodoElegido = string_itoa(nodoElegido->total);
		fputs(totalNodoElegido,archivoNodos);
		free(totalNodoElegido);
		fputc('\n',archivoNodos);

		fputs(nodoSeleccionado,archivoNodos);
		fputs("LIBRE=",archivoNodos);
		char* totalNodoLibre = string_itoa(nodoElegido->libre);
		fputs(totalNodoLibre ,archivoNodos);
		free(totalNodoLibre);
		fputc('\n',archivoNodos);

		s++;
	}
	fclose(archivoNodos);

}

int sacarPorcentajeOcioso(int bloquesLibres, int cantBloques){
	int numero = (bloquesLibres*100)/cantBloques;
	if((bloquesLibres*100)%cantBloques != 0){
	 numero++;
	}
	return numero;
}

void registrarNodo(int socket) {

	char * nombreNodo;
	char* ip;
	int cantBloques,puerto;
	t_bitarray * bitarray;

	nombreNodo = recibirString(socket);

	cantBloques = recibirUInt(socket);

	ip=recibirString(socket);
	puerto=recibirUInt(socket);


	// Checkeo estado anterior
	if(hayEstadoAnterior){
		//bitarray = abrirBitmap(nombreNodo,cantBloques);

	} else {
		bitarray = crearBitmap(nombreNodo,cantBloques);

	// Creo el bloque de info del nodo
	contenidoNodo* bloque = malloc(sizeof(contenidoNodo));
	bloque->nodo = string_new();
	string_append(&bloque->nodo, nombreNodo);
	bloque->total = cantBloques;
	bloque->libre = cantBloquesLibres(bitarray);
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

	//Aniado los datos de conexion del nodo
	datosConexionNodo* datosConexion=malloc(sizeof(datosConexionNodo));
	datosConexion->nodo=string_new();
	string_append(&datosConexion->nodo, nombreNodo);
	datosConexion->ip=string_new();
	string_append(&datosConexion->ip, ip);
	datosConexion->puerto=puerto;
	list_add(listaConexionNodos,datosConexion);

	persistirTablaNodo();

	}
	hayNodos++;

	free(ip);
	//free(nombreNodo);
}

//-----------------------------------------------FUNCION ALAMACENAR----------------------------------------------------
int sacarTamanioArchivo(FILE* archivo) {
	fseek(archivo, 0, SEEK_END);
	int tamanioArchivo = ftell(archivo);
	return tamanioArchivo;
}

int asignarBloqueNodo(contenidoNodo* nodo){
	int posicion=0;
	bool esNodo(tablaBitmapXNodos* nodoConBitmap){
		return(string_equals_ignore_case(nodoConBitmap->nodo,nodo->nodo));
	}
	tablaBitmapXNodos* nodoConbitarray = list_find(listaBitmap,(void*)esNodo);
	int tamaniobitarray = (bitarray_get_max_bit(nodoConbitarray->bitarray)/8);
	for(;posicion<tamaniobitarray;posicion++){
		if (!bitarray_test_bit(nodoConbitarray->bitarray, posicion)) {
			bitarray_set_bit(nodoConbitarray->bitarray,posicion);
			return posicion;
		}
	}
	return 0;
}

void asignarEnviarANodo(void* contenidoAEnviar,uint32_t tamanio,copiasXBloque* copiaBloque){
	int tabajoOcioso=0;
	void* mensaje=malloc(sizeof(uint32_t)*3+tamanio);
	uint32_t posicionActual=0;
	contenidoNodo* nodo0;
	contenidoNodo* nodo1;

	bool nodoMasOciosoCopia0(contenidoNodo* contenidoDelNodo){
		return (contenidoDelNodo->porcentajeOcioso>tabajoOcioso);
	}
	nodo0=list_find(tablaGlobalNodos->contenidoXNodo,(void*)nodoMasOciosoCopia0);
	nodo0->libre--;
	nodo0->porcentajeOcioso=sacarPorcentajeOcioso(nodo0->libre,nodo0->total);
	tablaGlobalNodos->libres--;

	bool nodoMasOciosoCopia1(contenidoNodo* contenidoDelNodo){
		return (contenidoDelNodo->porcentajeOcioso>tabajoOcioso && strcmp(contenidoDelNodo->nodo,nodo0->nodo)!=0);
	}
	nodo1=list_find(tablaGlobalNodos->contenidoXNodo,(void*)nodoMasOciosoCopia1);
	nodo1->libre--;
	nodo1->porcentajeOcioso=sacarPorcentajeOcioso(nodo1->libre,nodo1->total);
	tablaGlobalNodos->libres--;

	persistirTablaNodo();

	uint32_t bloqueAsignado=asignarBloqueNodo(nodo0);

	copiaBloque->copia1=malloc(sizeof(copia));

	copiaBloque->copia1->nodo=nodo0->nodo;
	copiaBloque->copia1->bloque=bloqueAsignado;


	memcpy(mensaje,&bloqueAsignado,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,&tamanio,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,&tamanio,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,contenidoAEnviar,tamanio);
	posicionActual+=tamanio;

	sendRemasterizado(nodo0->socket,ENV_ESCRIBIR,posicionActual,mensaje);
	if(recvDeNotificacion(nodo0->socket)==ESC_INCORRECTA){
		//CACHER ERROR
	}
//	free(mensaje);

	mensaje=realloc(mensaje,sizeof(uint32_t)*3+tamanio);

	bloqueAsignado=asignarBloqueNodo(nodo1);
	posicionActual = 0;
	memcpy(mensaje,&bloqueAsignado,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,&tamanio,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,&tamanio,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);

	memcpy(mensaje+posicionActual,contenidoAEnviar,tamanio);
	posicionActual+=tamanio;


	sendRemasterizado(nodo1->socket,ENV_ESCRIBIR,posicionActual,mensaje);

	copiaBloque->copia2=malloc(sizeof(copia));

	copiaBloque->copia2->nodo=nodo1->nodo;
	copiaBloque->copia2->bloque=bloqueAsignado;


	if(recvDeNotificacion(nodo1->socket)==ESC_INCORRECTA){
		//CACHER ERROR
	}
	free(mensaje);

}

void enviarDatosANodo(t_list* posiciones,FILE* archivo, tablaArchivos* archivoAGuardar) {
	uint32_t posicionActual = 0;
	void enviarNodoPorPosicion(int posicion) {
		copiasXBloque* copia=malloc(sizeof(copiasXBloque));
		if (posicionActual == 0) {
			void* contenido = malloc(posicion);
			fread(contenido, posicion, 1, archivo);
//			char* contenidoAEnviar = string_substring_until(contenido, posicion);
//			printf("%s", contenidoAEnviar);
			copia->bloque = string_new();
			char* numeroBloque = string_itoa(posicionActual);
			string_append(&copia->bloque, numeroBloque);
			copia->bytes=posicion;
			asignarEnviarANodo(contenido, posicion,copia);
//			free(contenidoAEnviar);
			free(contenido);
			free(numeroBloque);
		} else {
//			posicionActual--;
			uint32_t posicionAnterior = (int) list_get(posiciones,posicionActual-1);
			char* numeroBloque = string_itoa(posicionActual);
			copia->bloque = string_new();
			string_append(&copia->bloque, numeroBloque);
			copia->bytes=posicion- posicionAnterior;
			void* contenido = malloc(posicion - posicionAnterior);
			fread(contenido, posicion - posicionAnterior, 1,archivo);
//			char* contenidoAEnviar = string_substring_until(contenido, posicion-posicionAnterior);
			asignarEnviarANodo(contenido,posicion - posicionAnterior,copia);
			free(contenido);
			free(numeroBloque);
		}
		posicionActual++;
		list_add(archivoAGuardar->bloques,copia);
		list_add(tablaGlobalArchivos, archivoAGuardar);
	}
	list_iterate(posiciones,(void*) enviarNodoPorPosicion);
}

void almacenarArchivo(char* pathArchivo, char* pathDirectorio,char* tipo) {

	FILE* archivo = fopen(pathArchivo, "r+");//CACHEAR ERROR ARCHIVO
	//FILE* archivo = fopen("/home/utnso/workspace/tp-2017-2c-ElTPEstaBien/Master/Debug/yama-test1/WBAN.csv", "rb");
	uint32_t tamanio = sacarTamanioArchivo(archivo);
	t_list* posicionBloquesAGuardar=list_create();

	tablaArchivos* archivoAGuardar=malloc(sizeof(tablaArchivos));
	archivoAGuardar->bloques=list_create();

	char** ruta = string_split(pathArchivo,"/");
	archivoAGuardar->nombreArchivo=obtenerNombreDirectorio(ruta);
	archivoAGuardar->tipo=tipo;
	archivoAGuardar->directorioPadre=obtenerDirectorioPadre(ruta);
	liberarComandoDesarmado(ruta);
	uint32_t tamAux = 0;
		if(tablaGlobalNodos->libres*1024*1024>=(tamanio*2)){
			if(tamanio!=0){
				if (strcmp(tipo,"B")==0) {
					while (tamanio > 0) {
						if (tamanio < 1048576) {
							tamAux += tamanio;
							list_add(posicionBloquesAGuardar,tamAux);
							tamanio-=tamanio;
						} else {
							tamAux += 1048576;
							list_add(posicionBloquesAGuardar,tamAux);
							tamanio -= 1048576;
						}
					}
			} else {
				int digito;
				uint32_t ultimoBarraN = 0;
				uint32_t registroAntesMega = 0;
				uint32_t ultimaPosicion = 0;
				fseek(archivo, 0, SEEK_SET);
				while (!feof(archivo)) {
					digito = fgetc(archivo);

					if (digito == '\n') {
						ultimoBarraN = ftell(archivo);
					}

					if (ftell(archivo) == 1048576 + registroAntesMega) {
						registroAntesMega = ultimoBarraN;
						list_add(posicionBloquesAGuardar,ultimoBarraN);
					}
				}

				fseek(archivo, 0, SEEK_END);
				ultimaPosicion = ftell(archivo);
				list_add(posicionBloquesAGuardar, ultimaPosicion);
				fseek(archivo, 0, SEEK_SET);
			}
			enviarDatosANodo(posicionBloquesAGuardar,archivo,archivoAGuardar);
			list_destroy(posicionBloquesAGuardar);
		}else{
			log_error(loggerFileSystem,"El archivo se encuentra vacio");
		}

	}else{
		log_error(loggerFileSystem,"El tamaño del archivo supera la capacidad de almacenamiento del sistema");
	}

}

//------------------------------------------------LEER-------------------------------------------------------------
void leerArchivo(char* ruta,char* nombreArchivo){
	int cont=0;
	bool esArchivo(tablaArchivos* archivo){
		return(string_equals_ignore_case(archivo->nombreArchivo,nombreArchivo));
	}
	tablaArchivos* entradaArchivo = list_find(tablaGlobalArchivos,(void*)esArchivo);

	void* contenidoArchivo=malloc(entradaArchivo->tamanio);

	while(cont<list_size(entradaArchivo->bloques)){
		copiasXBloque* copiaBloque=(copiasXBloque*)list_get(entradaArchivo->bloques,cont);
		bool esNodo(contenidoNodo* nodoElegido){
			//return(strcmp(nodoElegido->nodo,copiaBloque->copia1->nodo)==0);
		}
		contenidoNodo* nodoSeleccionado=list_find(tablaGlobalNodos->contenidoXNodo,(void*)esNodo);
		//sendRemasterizado(nodoSeleccionado->socket,ENV_LEER,copiaBloque->)

	}

}

//--------------------------------YAMA----------------------------------------
//ENVIAR TABLA NODOS

int sacarTamanioMensaje() {
	int tamanio = 0;
	int i = 0;
	for (; i < list_size(tablaGlobalNodos->nodo); i++) {
		tamanio += sizeof(int);
		int tamanioNodo = string_length(list_get(tablaGlobalNodos->nodo, i));
		tamanio += tamanioNodo;
	}
	tamanio+=sizeof(uint32_t);
	return tamanio;
}

void enviarListaNodos(int socket) {
	void* mensaje = malloc(sacarTamanioMensaje());
	int posicionActual = 0;
	int i = 0;
	int cantNodo=list_size(tablaGlobalNodos->nodo);
	memcpy(mensaje,&cantNodo,sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	for (; i < list_size(tablaGlobalNodos->nodo); i++) {
		int tamanioNodo = string_length(list_get(tablaGlobalNodos->nodo, i));
		memcpy(mensaje+posicionActual, &tamanioNodo, sizeof(int));
		posicionActual += sizeof(int);
		memcpy(mensaje + posicionActual, list_get(tablaGlobalNodos->nodo, i),
				tamanioNodo);
		posicionActual += tamanioNodo;
	}
	int tamanioMsj = sacarTamanioMensaje();
	sendRemasterizado(socket, ES_FS, tamanioMsj, mensaje);
	free(mensaje);
}

//ENVIAR DATOS

int tamanioStruct(copiasXBloque* contenido){
	return sizeof(uint32_t)*6 + string_length(contenido->copia1->nodo) + string_length(contenido->copia2->nodo);
}

void* serializarCopiaBloque(copiasXBloque* contenido){
	int posicionActual = 0;

	void* datosSerializados = malloc(tamanioStruct(contenido));

	// serializo bloque
	uint32_t nroBloque = atoi(contenido->bloque);
	memcpy(datosSerializados + posicionActual, &nroBloque, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	//serializo copia1
	uint32_t tamanioCopia1 = string_length(contenido->copia1->nodo);
	memcpy(datosSerializados + posicionActual, &tamanioCopia1, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(datosSerializados + posicionActual, contenido->copia1->nodo, tamanioCopia1);
	posicionActual += tamanioCopia1;
	memcpy(datosSerializados + posicionActual, &contenido->copia1->bloque, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	// serializo copia2
	uint32_t tamanioCopia2 = string_length(contenido->copia2->nodo);
	memcpy(datosSerializados + posicionActual, &tamanioCopia2, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	memcpy(datosSerializados + posicionActual, contenido->copia2->nodo, tamanioCopia2);
	posicionActual += tamanioCopia2;
	memcpy(datosSerializados + posicionActual, &contenido->copia2->bloque, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);
	//serializo bytes
	memcpy(datosSerializados + posicionActual, &contenido->bytes, sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);

	return datosSerializados;

}

int sacarTamanio(tablaArchivos* entradaArchivo){
	uint32_t cont=0;
	uint32_t posicionActual=0;
	copiasXBloque* copiaBloque;
	while(cont<list_size(entradaArchivo->bloques)){
		copiaBloque=(copiasXBloque*)list_get(entradaArchivo->bloques,cont);
		posicionActual+=tamanioStruct(copiaBloque);
		cont++;
	}
//	free(copiaBloque);
	return posicionActual;
}


void enviarTablaAYama(int socket, tablaArchivos* entradaArchivo){
	uint32_t cont=0;
	uint32_t posicionActual=0;

	void* mensaje= malloc(sizeof(uint32_t)+sacarTamanio(entradaArchivo));

	int cantBloques=list_size(entradaArchivo->bloques);
	memcpy(mensaje,&cantBloques,sizeof(uint32_t));
	posicionActual += sizeof(uint32_t);

	while(cont<list_size(entradaArchivo->bloques)){
		copiasXBloque* copiaBloque=(copiasXBloque*)list_get(entradaArchivo->bloques,cont);
		void* espacioASerializar = serializarCopiaBloque(copiaBloque);
		memcpy(mensaje+posicionActual,espacioASerializar,tamanioStruct(copiaBloque));
		posicionActual+=tamanioStruct(copiaBloque);
		free(espacioASerializar);

		cont++;
	}

	sendRemasterizado(socket, INFO_ARCHIVO_FS, posicionActual, mensaje);
}


void enviarDatoArchivo(int socket){
  char* archivoABuscar=recibirString(socket);
	bool esArchivo(tablaArchivos* archivo){
		return(string_equals_ignore_case(archivo->nombreArchivo,archivoABuscar));
	}
	tablaArchivos* entradaArchivo = list_find(tablaGlobalArchivos,(void*)esArchivo);
	if(entradaArchivo==NULL){
		sendDeNotificacion(socket,ARCHIVO_NO_ENCONTRADO);
	}
	enviarTablaAYama(socket,entradaArchivo);
}

//ENVIAR IP Y PUERTO
void enviarDatosConexionNodo(int socket){
	char* nodo=string_new();
	nodo=recibirString(socket);
	bool esNodo(datosConexionNodo* nodoSeleccionado){
		return(string_equals_ignore_case(nodoSeleccionado->nodo,nodo));
	}
	datosConexionNodo* datosNodo = list_find(listaConexionNodos,(void*)esNodo);
	void* mensaje=malloc(sizeof(uint32_t)*2+string_length(datosNodo->ip));
	int tamanioIp=string_length(datosNodo->ip);
	int posicionActual=0;

	memcpy(mensaje,&datosNodo->puerto,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);
	memcpy(mensaje+posicionActual,&tamanioIp,sizeof(uint32_t));
	posicionActual+=sizeof(uint32_t);
	memcpy(mensaje+posicionActual,datosNodo->ip,tamanioIp);
	posicionActual+=tamanioIp;

	sendRemasterizado(socket,DATOS_NODO,posicionActual,mensaje);

}
//--------------------------------Worker----------------------------------------
void almacenarArchivoWorker(int socket){
	char* contenido = string_new();
	char* path = string_new();
	char* nombreArchivo = string_new();
	uint32_t tipo;
	contenido = recibirString(socket);
	nombreArchivo = recibirString(socket);
	path = recibirString(socket);
	tipo = recibirUInt(socket);
	FILE* archivo=fopen(nombreArchivo,"r+");
	fputs(contenido,archivo);
	fclose(archivo);
	if(tipo==21){
		almacenarArchivo(nombreArchivo,path,"B");
	}
	if(tipo==22){
		almacenarArchivo(nombreArchivo,path,"T");
	}
}


//--------------------------------Main----------------------------------------
int main(int argc, char **argv) {
	loggerFileSystem = log_create("FileSystem.log", "FileSystem", 1, 0);
	chequearParametros(argc, 2);
	t_config* configuracionFS = generarTConfig(argv[1], 2);
//	t_config* configuracionFS = generarTConfig("Debug/filesystem.ini", 2);
	cargarFileSystem(configuracionFS);
	int socketMaximo, socketClienteChequeado, socketAceptado;
	int socketEscuchaFS = ponerseAEscucharClientes(PUERTO_ESCUCHA, 0);
	pthread_t hiloConsolaFS;
	socketMaximo = socketEscuchaFS;
	fd_set socketClientes, socketClientesAuxiliares;
	FD_ZERO(&socketClientes);
	FD_ZERO(&socketClientesAuxiliares);
	FD_SET(socketEscuchaFS, &socketClientes);
	pthread_create(&hiloConsolaFS, NULL, (void*) consolaFS, NULL);

	tablaGlobalNodos = malloc(sizeof(tablaNodos));
	tablaGlobalNodos->libres=0;
	tablaGlobalNodos->tamanio=0;
	tablaGlobalNodos->nodo = list_create();
	tablaGlobalNodos->contenidoXNodo = list_create();

	listaBitmap = list_create();
	tablaGlobalArchivos = list_create();
	listaConexionNodos=list_create();
	listaDirectorios = list_create();

	hayNodos=0;
	esEstadoSeguro=true;


	while (1) {
		socketClientesAuxiliares = socketClientes;
		if (select(socketMaximo + 1, &socketClientesAuxiliares, NULL, NULL,
		NULL) == -1) {
			log_error(loggerFileSystem, "No se pudo llevar a cabo el select.");
			exit(-1);
		}
		log_info(loggerFileSystem,
				"Se recibio nueva actividad de los clientes");
		for (socketClienteChequeado = 0; socketClienteChequeado <= socketMaximo;
				socketClienteChequeado++) {
			if (FD_ISSET(socketClienteChequeado, &socketClientesAuxiliares)) {
				if (socketClienteChequeado == socketEscuchaFS) {
					socketAceptado = aceptarConexionDeCliente(socketEscuchaFS);
					FD_SET(socketAceptado, &socketClientes);
					socketMaximo = calcularSocketMaximo(socketAceptado,
							socketMaximo);
					log_info(loggerFileSystem,
							"Se ha recibido una nueva conexion.");
				} else {
					int notificacion = recvDeNotificacion(
							socketClienteChequeado);
					switch (notificacion) {
					case ES_DATANODE:
						sendDeNotificacion(socketClienteChequeado, ES_FS);
						break;
					case ES_YAMA:
						if (hayNodos==2 && esEstadoSeguro) {
							enviarListaNodos(socketClienteChequeado);
						} else {
							FD_CLR(socketClienteChequeado, &socketClientes);
							close(socketClienteChequeado);
						}
						break;
					case REC_INFONODO:
						registrarNodo(socketClienteChequeado);
						break;
					case REC_BLOQUE:
						recibirBloque(socketClienteChequeado);
						break;
					case INFO_ARCHIVO_FS:
						enviarDatoArchivo(socketClienteChequeado);
						break;
					case DATOS_NODO:
						enviarDatosConexionNodo(socketClienteChequeado);
						break;
					case ALMACENADO_FINAL:
						almacenarArchivoWorker(socketClienteChequeado);
						break;
					default:
						log_error(loggerFileSystem,
								"La conexion recibida es erronea.");
						FD_CLR(socketClienteChequeado, &socketClientes);
						close(socketClienteChequeado);
					}

				}
			}
		}
	}
	return EXIT_SUCCESS;
}
