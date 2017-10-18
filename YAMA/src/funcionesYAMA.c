
#include "funcionesYAMA.h"

//GENERACION DE ESTRUCTURAS
administracionYAMA* generarAdministracion(){
	administracionYAMA* adminNuevo = malloc(sizeof(administracionYAMA));
	adminNuevo->nombreNodo = string_new();
	adminNuevo->nameFile = string_new();
	adminNuevo->estado = EN_PROCESO;
	return adminNuevo;
}

conexionNodo* generarConexionNodo(){
	conexionNodo* conexion = malloc(sizeof(conexionNodo));
	conexion->ipNodo = string_new();
	conexion->nombreNodo = string_new();
	return conexion;
}

copia* generarCopia(){
	copia* copiaNueva = malloc(sizeof(copia));
	copiaNueva->nombreNodo = string_new();
	return copiaNueva;
}

infoDeFs* generarInformacionDeBloque(){
	infoDeFs* informacion = malloc(sizeof(infoDeFs));
	informacion->copia1 = malloc(sizeof(copia));
	informacion->copia2 = malloc(sizeof(copia));
	informacion->copia1->nombreNodo = string_new();
	informacion->copia2->nombreNodo = string_new();
	return informacion;
}

nodoSistema* generarNodoSistema(){
	nodoSistema* nodo = malloc(sizeof(nodoSistema));
	nodo->nombreNodo = string_new();
	return nodo;
}

//LIBERACION DE ESTRUCTURAS
void liberarConexion(conexionNodo* conexion){
	free(conexion->ipNodo);
	free(conexion->nombreNodo);
	free(conexion);
}

void liberarInfoFS(infoDeFs* infoDeBloque){
	free(infoDeBloque->copia1->nombreNodo);
	free(infoDeBloque->copia2->nombreNodo);
	free(infoDeBloque->copia1);
	free(infoDeBloque->copia2);
	free(infoDeBloque);
}

void liberarCopia(copia* copiaAEnviar) {
	free(copiaAEnviar->nombreNodo);
	free(copiaAEnviar);
}

//RANDOM NAMES
char* obtenerNombreTemporalLocal(){
	char* nombreArchivo = string_new();
	string_append(&nombreArchivo, "tempFileLocal");
	string_append(&nombreArchivo, string_itoa(numeroDeTemporalLocal));
	numeroDeTemporalLocal++;
	return nombreArchivo;
}

char* obtenerNombreTemporalGlobal(){
	char* nombreArchivo = string_new();
	string_append(&nombreArchivo, "tempFileGlobal");
	string_append(&nombreArchivo, string_itoa(numeroDeTemporalGlobal));
	numeroDeTemporalGlobal++;
	return nombreArchivo;
}

char* obtenerNombreTemporalTransformacion(){
	char* nombreArchivo = string_new();
	string_append(&nombreArchivo, "tempFileTransformacion");
	string_append(&nombreArchivo, string_itoa(numeroDeTemporalTransformacion));
	numeroDeTemporalTransformacion++;
	return nombreArchivo;
}

int obtenerNumeroDeJob(){
	contadorDeJobs++;
	return contadorDeJobs;
}

int obtenerNumeroDeMaster(){
	contadorDeMasters++;
	return contadorDeMasters;
}

//OBTENER DATOS DE CONEXION CON NODO

void deserializarIPYPuerto(conexionNodo* conexion){
	if(recibirInt(socketFS) != DATOS_NODO){
		log_error(loggerYAMA, "Error al recibir la IP y el puerto del nodo.");
		exit(-1);
	}
	conexion->puertoNodo = recibirInt(socketFS);
	conexion->ipNodo = recibirString(socketFS);
}

void obtenerIPYPuerto(conexionNodo* conexion){
	void* mensaje = malloc(sizeof(int)+string_length(conexion->nombreNodo));
	int tamanio = string_length(conexion->nombreNodo);
	memcpy(mensaje, &tamanio, sizeof(int));
	memcpy(mensaje + sizeof(int), conexion->nombreNodo, tamanio);
	sendRemasterizado(socketFS, DATOS_NODO, sizeof(int)+tamanio, mensaje);
	free(mensaje);
	deserializarIPYPuerto(conexion);
}

//GETTERS
//GETTERS NODOS
int obtenerJobDeNodo(t_list* listaDelNodo){
	administracionYAMA* admin = list_get(listaDelNodo, 0);
	return admin->nroJob;
}

t_list* obtenerListaDelNodo(int nroMaster, int socketMaster){
	char* nombreNodo = recibirString(socketMaster);
	bool esDeNodo(administracionYAMA* admin){
		return (strcmp(nombreNodo, admin->nombreNodo) && admin->nroMaster == nroMaster);
	}
	t_list* listaDelNodo = list_filter(tablaDeEstados, (void*)esDeNodo);
	free(nombreNodo);
	return listaDelNodo;
}

char* obtenerNombreNodo(t_list* listaDelNodo){
	administracionYAMA* admin = list_get(tablaDeEstados, 0);
	return admin->nombreNodo;
}

//PUESTA EN MARCHA DE YAMA
void cargarYAMA(t_config* configuracionYAMA){
    if(!config_has_property(configuracionYAMA, "FS_IP")){
        log_error(loggerYAMA, "No se encuentra el parametro FS_IP en el archivo de configuracion");
        exit(-1);
    }
    FS_IP = string_new();
	string_append(&FS_IP, config_get_string_value(configuracionYAMA, "FS_IP"));
    if(!config_has_property(configuracionYAMA, "FS_PUERTO")){
        log_error(loggerYAMA, "No se encuentra el parametro FS_PUERTO en el archivo de configuracion");
        exit(-1);
    }
    FS_PUERTO = config_get_int_value(configuracionYAMA, "FS_PUERTO");
    if(!config_has_property(configuracionYAMA, "RETARDO_PLANIFICACION")){
        log_error(loggerYAMA, "No se encuentra el parametro RETARDO_PLANIFICACION en el archivo de configuracion");
        exit(-1);
    }
    RETARDO_PLANIFICACION = config_get_int_value(configuracionYAMA, "RETARDO_PLANIFICACION");
    if(!config_has_property(configuracionYAMA, "ALGORITMO_BALANCEO")){
    	log_error(loggerYAMA, "No se encuentra el parametro RETARDO_PLANIFICACION en el archivo de configuracion");
    	exit(-1);
    }
    ALGORITMO_BALANCEO = string_new();
    string_append(&ALGORITMO_BALANCEO, config_get_string_value(configuracionYAMA, "ALGORITMO_BALANCEO"));
    if(!config_has_property(configuracionYAMA, "PUERTO_MASTERS")){
    	log_error(loggerYAMA, "No se encuentra el parametro PUERTO_MASTERS en el archivo de configuracion");
    	exit(-1);
    }
    PUERTO_MASTERS = config_get_int_value(configuracionYAMA, "PUERTO_MASTERS");
    if(!config_has_property(configuracionYAMA, "BASE_AVAILABILITY")){
    	log_error(loggerYAMA, "No se encuentra el parametro BASE_AVAILABILITY en el archivo de configuracion");
    	exit(-1);
    }
    BASE_AVAILABILITY = config_get_int_value(configuracionYAMA, "BASE_AVAILABILITY");
    config_destroy(configuracionYAMA);
}


//CHEQUEO DE SIGNAL
void chequeameLaSignal(int signal){
	//PASO A RECARGAR EL ARCHIVO
	log_info(loggerYAMA, "Se ha recibido la signal SIGUSR1");
	t_config* configuracionNueva = generarTConfig("Debug/yama.ini", 5);
	if(!config_has_property(configuracionNueva, "RETARDO_PLANIFICACION")){
		log_error(loggerYAMA, "Al recargar la configuracion no se encontro RETARDO_PLANIFICACION en el archivo.");
		exit(-1);
	}else{
		RETARDO_PLANIFICACION = config_get_int_value(configuracionNueva, "RETARDO_PLANIFICACION");
		log_info(loggerYAMA, "Se ha actualizado el valor de RETARDO_PLANIFICACION a %d", RETARDO_PLANIFICACION);
	}
	if(!config_has_property(configuracionNueva, "ALGORITMO_BALANCEO")){
		log_error(loggerYAMA, "Al recargar la configuracion no se encontro ALGORITMO_BALANCEO en el archivo");
		exit(-1);
	}else{
		ALGORITMO_BALANCEO = string_new();
		string_append(&ALGORITMO_BALANCEO, config_get_string_value(configuracionNueva, "ALGORITMO_BALANCEO"));
		log_info(loggerYAMA, "Se ha actualizado el valor de ALGORITMO_BALANCEO a %s", ALGORITMO_BALANCEO);
	}
	config_destroy(configuracionNueva);
}

//ENVIO DE MENSAJES
void enviarCopiaAMaster(int socket, copia* copiaAEnviar){
	conexionNodo* conection = generarConexionNodo();
	string_append(&conection->nombreNodo, copiaAEnviar->nombreNodo);
	obtenerIPYPuerto(conection);
	void* copiaSerializada = serializarCopia(copiaAEnviar, conection);
	sendRemasterizado(socket, REPLANIFICAR, obtenerTamanioCopia(copiaAEnviar, conection), copiaSerializada);
	liberarCopia(copiaAEnviar);
}

//HANDSHAKE CON FS (RECIBO LOS NODOS DEL SISTEMA)
void handshakeFS(){
	sendDeNotificacion(socketFS, ES_YAMA);
	if(recibirInt(socketFS) != ES_FS){
		log_error(loggerYAMA, "La conexion efectuada no es con FileSystem.");
		exit(-1);
	}
	int cantidadDeNodos = recibirInt(socketFS);
	int i;
	for(i = 0; i<cantidadDeNodos; i++){
		nodoSistema* nuevoNodo = generarNodoSistema();
		nuevoNodo->nombreNodo = recibirString(socketFS);
		nuevoNodo->wl = 0;
		list_add(nodosSistema, nuevoNodo);
	}
}

//FUNCIONES PARA MANEJAR LA AVAILABILITY
int obtenerWLMax(){
	int maximo = 0;
	bool maximoWL(nodoSistema* nodoAChequear){
		if(nodoAChequear->wl>maximo){
			maximo = nodoAChequear->wl;
			return true;
		}else{
			return false;
		}
	}
	nodoSistema* nodo = list_find(nodosSistema, (void*)maximoWL);
	return nodo->wl;
}

int calculoAvailability(char* nombreNodo){
	bool esNodo(nodoSistema* nodoAChequear){
		return strcmp(nombreNodo, nodoAChequear->nombreNodo);
	}
	int availability = 0;
	if(strcmp(ALGORITMO_BALANCEO, "Clock")){
		availability = BASE_AVAILABILITY;
	}else{
		int wlMax = obtenerWLMax();
		nodoSistema* nodo = list_find(nodosSistema, (void*)esNodo);
		availability = wlMax - nodo->wl;
	}
	return availability;
}

//ARMO LOS DATOS DE BALANCEO A PARTIR DE LOS DATOS RECIBIDOS DE LA LISTA DE FS
datosBalanceo* obtenerDatosDeCopia(t_list* listaDeBalanceo, copia* copiaAChequear){
	bool existeEnLaLista(datosBalanceo* datos){
		return strcmp(datos->nombreNodo, copiaAChequear->nombreNodo);
	}
	return list_find(listaDeBalanceo, (void*)existeEnLaLista);
}

datosBalanceo* generarDatosBalanceo(){
	datosBalanceo* datos = malloc(sizeof(datosBalanceo));
	datos->nombreNodo = string_new();
	datos->bloques = list_create();
	return datos;
}

t_list* armarDatosBalanceo(t_list* listaDeBloques){
	bool porMayorAvailability(datosBalanceo* dato1, datosBalanceo* datos2){
		return dato1->availability > datos2->availability;
	}
	int posicion;
	t_list* listaDeBalanceo = list_create();
	for(posicion = 0; posicion < list_size(listaDeBloques); posicion++){
		infoDeFs* informacionAOrdenar = list_get(listaDeBloques, posicion);
		datosBalanceo* datoCopia1 = obtenerDatosDeCopia(listaDeBalanceo, informacionAOrdenar->copia1);
		datosBalanceo* datoCopia2 = obtenerDatosDeCopia(listaDeBalanceo, informacionAOrdenar->copia2);
		if(datoCopia1 != NULL){
			list_add(datoCopia1->bloques, &informacionAOrdenar->nroBloque);
		}else{
			datosBalanceo* datosAAgregar = generarDatosBalanceo();
			datosAAgregar->nombreNodo = informacionAOrdenar->copia1->nombreNodo;
			datosAAgregar->availability = calculoAvailability(datosAAgregar->nombreNodo);
			list_add(datosAAgregar->bloques, &informacionAOrdenar->nroBloque);
			list_add(listaDeBalanceo, datosAAgregar);
		}
		if(datoCopia2 != NULL){
			list_add(datoCopia2->bloques, &informacionAOrdenar->nroBloque);
		}else{
			datosBalanceo* datosAAgregar = generarDatosBalanceo();
			datosAAgregar->nombreNodo = informacionAOrdenar->copia2->nombreNodo;
			datosAAgregar->availability = calculoAvailability(datosAAgregar->nombreNodo);
			list_add(datosAAgregar->bloques, &informacionAOrdenar->nroBloque);
			list_add(listaDeBalanceo, datosAAgregar);
		}
	}
	list_sort(listaDeBalanceo, (void*)porMayorAvailability); //ORDENO LA LISTA BALANCEO A PARTIR DEL availability
	return listaDeBalanceo;
}


