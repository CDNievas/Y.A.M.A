#include "funcionesYAMA.h"

//GENERACION DE ESTRUCTURAS
administracionYAMA* generarAdministracion(uint32_t nroJob, uint32_t nroMaster, uint32_t operacion, char* nameFile){
	administracionYAMA* adminNuevo = malloc(sizeof(administracionYAMA));
	adminNuevo->nameFile = nameFile;
	adminNuevo->estado = EN_PROCESO;
	adminNuevo->etapa = operacion;
	adminNuevo->nroJob = nroJob;
	adminNuevo->nroMaster = nroMaster;
	return adminNuevo;
}

conexionNodo* generarConexionNodo(){
	conexionNodo* conexion = malloc(sizeof(conexionNodo));
	return conexion;
}

infoDeFs* generarInformacionDeBloque(){
	infoDeFs* informacion = malloc(sizeof(infoDeFs));
	informacion->copia1 = malloc(sizeof(copia));
	informacion->copia2 = malloc(sizeof(copia));
	return informacion;
}

nodoSistema* generarNodoSistema(){
	nodoSistema* nodo = malloc(sizeof(nodoSistema));
	return nodo;
}

//LIBERACION DE ESTRUCTURAS
void liberarDatoMaster(infoNodo* info){
	free(info->nombreTemporal);
	liberarConexion(info->conexion);
	free(info);
}

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

void liberarInfoNodo(infoNodo* info){
	free(info->nombreTemporal);
	liberarConexion(info->conexion);
	free(info);
}

void liberarDatosBalanceo(datosBalanceo* datos){
	free(datos->nombreNodo);
	list_destroy(datos->bloques);
	free(datos);
}

void liberarNodoSistema(nodoSistema* nodo){
	free(nodo->nombreNodo);
	free(nodo);
}

void liberarAdminYAMA(administracionYAMA* admin){
	free(admin->nameFile);
	free(admin->nombreNodo);
	free(admin);
}

//RANDOM NAMES
char* obtenerNombreTemporalLocal(){
	char* nombreArchivo = string_new();

	pthread_mutex_lock(&semReducLocales);
	int nro = numeroDeTemporalLocal;
	numeroDeTemporalLocal++;
	pthread_mutex_unlock(&semReducLocales);

	string_append(&nombreArchivo, "tempFileLocal");
	char* numero = string_itoa(nro);
	string_append(&nombreArchivo, numero);

	free(numero);
	return nombreArchivo;
}

char* obtenerNombreTemporalGlobal(){
	char* nombreArchivo = string_new();

	pthread_mutex_lock(&semReducGlobales);
	int nro = numeroDeTemporalGlobal;
	numeroDeTemporalGlobal++;
	pthread_mutex_unlock(&semReducGlobales);

	string_append(&nombreArchivo, "tempFileGlobal");
	char* numero = string_itoa(nro);
	string_append(&nombreArchivo, numero);
	free(numero);
	return nombreArchivo;
}

char* obtenerNombreTemporalTransformacion(){
	char* nombreArchivo = string_new();

	pthread_mutex_lock(&semTransformaciones);
	int nro = numeroDeTemporalTransformacion;
	numeroDeTemporalTransformacion++;
	pthread_mutex_unlock(&semTransformaciones);

	char* numero = string_itoa(nro);
	string_append(&nombreArchivo, "tempFileTransformacion");
	string_append(&nombreArchivo, numero);
	free(numero);
	return nombreArchivo;
}

int obtenerNumeroDeJob(){

	pthread_mutex_lock(&semContJobs);
	contadorDeJobs++;
	int nroCont = contadorDeJobs;
	pthread_mutex_unlock(&semContJobs);

	return nroCont;
}

int obtenerNumeroDeMaster(){

	pthread_mutex_lock(&semContMaster);
	contadorDeMasters++;
	int nroCont = contadorDeMasters;
	pthread_mutex_unlock(&semContMaster);

	return nroCont;
}

//OBTENER DATOS DE CONEXION CON NODO

void deserializarIPYPuerto(conexionNodo* conexion){
	if(recibirUInt(socketFS) != DATOS_NODO){
		log_error(loggerYAMA, "Error al recibir la IP y el puerto del nodo.");
		conexion->puertoNodo = -1;
		conexion->ipNodo = NULL;
	}else{
		conexion->puertoNodo = recibirUInt(socketFS);
		conexion->ipNodo = recibirString(socketFS);
	}
}

void obtenerIPYPuerto(conexionNodo* conexion){
	void* mensaje = malloc(sizeof(int)+string_length(conexion->nombreNodo));
	uint32_t tamanio = string_length(conexion->nombreNodo);
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

t_list* obtenerListaDelNodo(int nroMaster, int socketMaster, char* nombreNodo){
	bool esDeNodo(administracionYAMA* admin){
		return (!strcmp(nombreNodo, admin->nombreNodo) && admin->nroMaster == nroMaster && admin->etapa == TRANSFORMACION && admin->estado != FALLO);
	}
	pthread_mutex_lock(&semTablaEstados);
	t_list* listaDelNodo = list_filter(tablaDeEstados, (void*)esDeNodo);
	pthread_mutex_unlock(&semTablaEstados);
	return listaDelNodo;
}

char* obtenerNombreNodo(t_list* listaDelNodo){
	administracionYAMA* admin = list_get(listaDelNodo, 0);
	char* nombreNodo = string_new();
	string_append(&nombreNodo, admin->nombreNodo);
	return nombreNodo;
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

void laParca(int signal){
	//Se prosigue a morir elegantemente
	log_info(loggerYAMA, "Se recibio SIGINT.");
	log_info(loggerYAMA, "Muriendo con estilo.");
	list_destroy_and_destroy_elements(nodosSistema, (void*)liberarNodoSistema);
	list_destroy_and_destroy_elements(tablaDeEstados, (void*)liberarAdminYAMA);
	free(FS_IP);
	free(ALGORITMO_BALANCEO);
	log_info(loggerYAMA, "Todas las estructuras liberadas.");
	log_info(loggerYAMA, "Cerrando sockets.");
	close(socketEscuchaMasters);
	close(socketFS);
	log_info(loggerYAMA, "Muriendo...");
	log_destroy(loggerYAMA);
	exit(0);
}

//ENVIO DE MENSAJES
//void enviarCopiaAMaster(int socket, copia* copiaAEnviar){
//	conexionNodo* conection = generarConexionNodo();
//	string_append(&conection->nombreNodo, copiaAEnviar->nombreNodo);
//	obtenerIPYPuerto(conection);
//	void* copiaSerializada = serializarCopia(copiaAEnviar, conection);
//	sendRemasterizado(socket, REPLANIFICAR, obtenerTamanioCopia(copiaAEnviar, conection), copiaSerializada);
//	liberarCopia(copiaAEnviar);
//}

//HANDSHAKE CON FS (RECIBO LOS NODOS DEL SISTEMA)
void handshakeFS(){
	sendDeNotificacion(socketFS, ES_YAMA);
	if(recibirUInt(socketFS) != ES_FS){
		log_error(loggerYAMA, "La conexion efectuada no es con FileSystem.");
		exit(-1);
	}
	uint32_t cantidadDeNodos = recibirUInt(socketFS);
	log_info(loggerYAMA, "Cantidad de nodos del sistema: %d", cantidadDeNodos);
	uint32_t i;
	for(i = 0; i<cantidadDeNodos; i++){
		nodoSistema* nuevoNodo = generarNodoSistema();
		nuevoNodo->nombreNodo = recibirString(socketFS);
		nuevoNodo->wl = 0;
		list_add(nodosSistema, nuevoNodo);
	}
	log_info(loggerYAMA, "Se han recibido todos los nodos del sistema correctamente.");
}

//FUNCIONES PARA MANEJAR LA AVAILABILITY
int obtenerWLMax(){
	uint32_t maximo = 0;
	bool maximoWL(nodoSistema* nodoAChequear){
		if(nodoAChequear->wl>=maximo){
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
		return strcmp(nombreNodo, nodoAChequear->nombreNodo) == 0;
	}
	uint32_t availability = 0;
	if(strcmp(ALGORITMO_BALANCEO, "Clock") == 0){
		availability = BASE_AVAILABILITY;
	}else{
		int wlMax = obtenerWLMax();
		nodoSistema* nodo = list_find(nodosSistema, (void*)esNodo);
		availability = BASE_AVAILABILITY + wlMax - nodo->wl;
	}
	return availability;
}

//ARMO LOS DATOS DE BALANCEO A PARTIR DE LOS DATOS RECIBIDOS DE LA LISTA DE FS
datosBalanceo* obtenerDatosDeCopia(t_list* listaDeBalanceo, copia* copiaAChequear){
	bool existeEnLaLista(datosBalanceo* datos){
		return strcmp(datos->nombreNodo, copiaAChequear->nombreNodo) == 0;
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
	uint32_t posicion;
	t_list* listaDeBalanceo = list_create();
	for(posicion = 0; posicion < list_size(listaDeBloques); posicion++){
		infoDeFs* informacionAOrdenar = list_get(listaDeBloques, posicion);
		datosBalanceo* datoCopia1 = obtenerDatosDeCopia(listaDeBalanceo, informacionAOrdenar->copia1);
		datosBalanceo* datoCopia2 = obtenerDatosDeCopia(listaDeBalanceo, informacionAOrdenar->copia2);
		if(datoCopia1 != NULL){
			list_add(datoCopia1->bloques, informacionAOrdenar->nroBloque);
		}else{
			datosBalanceo* datosAAgregar = generarDatosBalanceo();
			string_append(&datosAAgregar->nombreNodo, informacionAOrdenar->copia1->nombreNodo);
			datosAAgregar->availability = calculoAvailability(datosAAgregar->nombreNodo);
			list_add(datosAAgregar->bloques, informacionAOrdenar->nroBloque);
			list_add(listaDeBalanceo, datosAAgregar);
		}
		if(datoCopia2 != NULL){
			list_add(datoCopia2->bloques, informacionAOrdenar->nroBloque);
		}else{
			datosBalanceo* datosAAgregar = generarDatosBalanceo();
			string_append(&datosAAgregar->nombreNodo, informacionAOrdenar->copia2->nombreNodo);
			datosAAgregar->availability = calculoAvailability(datosAAgregar->nombreNodo);
			list_add(datosAAgregar->bloques, informacionAOrdenar->nroBloque);
			list_add(listaDeBalanceo, datosAAgregar);
		}
	}
	list_sort(listaDeBalanceo, (void*)porMayorAvailability); //ORDENO LA LISTA BALANCEO A PARTIR DEL availability
	return listaDeBalanceo;
}


char* buscarNodoEncargado(uint32_t nroMaster){
	bool esReduccionFinalizada(administracionYAMA* admin){
		return admin->nroMaster == nroMaster && admin->estado == FINALIZADO && admin->etapa == REDUCCION_GLOBAL;
	}
	pthread_mutex_lock(&semTablaEstados);
	administracionYAMA* admin = list_find(tablaDeEstados, (void*)esReduccionFinalizada);
	char* nodoEncargado = admin->nombreNodo;
	pthread_mutex_unlock(&semTablaEstados);
	return nodoEncargado;
}


t_list* filtrarTablaMaster(uint32_t nroMaster){
  bool esDeMaster(administracionYAMA* admin){
    return admin->nroMaster == nroMaster;
  }
  t_list* listaMaster = list_filter(tablaDeEstados, (void*)esDeMaster);
  return listaMaster;
}
