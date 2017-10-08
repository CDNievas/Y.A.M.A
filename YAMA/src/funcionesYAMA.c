
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
  paquete* paqueteDeFS = recvRemasterizado(socketFS);
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

void realizarHandshakeConFS(int socketFS){
	sendDeNotificacion(socketFS, ES_YAMA);
	int notificacion = recvDeNotificacion(socketFS);
	if(notificacion != ES_FS){
		log_error(loggerYAMA, "La conexion establecida no es con FileSystem");
		exit(-1);
	}
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


