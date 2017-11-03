#include "funcionesMaster.h"
#include "structMaster.h"

void cargarMaster(t_config* configuracionMaster){
    if(!config_has_property(configuracionMaster, "YAMA_IP")){
        log_error(loggerMaster, "No se encuentra YAMA_IP");
        exit(-1);
    }else{
        YAMA_IP = string_new();
        string_append(&YAMA_IP, config_get_string_value(configuracionMaster, "YAMA_IP"));
    }
    if(!config_has_property(configuracionMaster, "YAMA_PUERTO")){
        log_error(loggerMaster, "No se encuentra YAMA_PUERTO en el archivo de configuracion");
        exit(-1);
    }else{
        YAMA_PUERTO = config_get_int_value(configuracionMaster, "YAMA_PUERTO");
    }
}

void realizarHandshake(int unSocket, int proceso){
    sendDeNotificacion(unSocket, ES_MASTER);
    int notificacion = recvDeNotificacion(unSocket);
    if(notificacion != proceso){
        log_error(loggerMaster, "La conexion establecida no es correcta");
        exit(-1);
    }
}


long int obtenerTamanioArchivo(FILE* unArchivo){
	fseek(unArchivo, 0, SEEK_END);
	long int tamanioArchivo = ftell(unArchivo);
	return tamanioArchivo;
}

char* leerArchivo(FILE* unArchivo, long int tamanioArchivo)
{
	fseek(unArchivo, 0, SEEK_SET);
	char* contenidoArchivo = malloc(tamanioArchivo+1);
	fread(contenidoArchivo, tamanioArchivo, 1, unArchivo);
	contenidoArchivo[tamanioArchivo] = '\0';
	return contenidoArchivo;
}

char* obtenerContenido(char* unPath){
	FILE* archivoALeer = fopen(unPath, "rb");
	long int tamanioArchivo = obtenerTamanioArchivo(archivoALeer);
	char* contenidoArchivo = leerArchivo(archivoALeer,tamanioArchivo);
	return contenidoArchivo;
}
/*
void propagarArchivo(char* unNombreArchivo, int socketDeYama, int tipoMensaje){
	char* contenidoArchivo = obtenerContenido(unNombreArchivo);
	sendRemasterizado(socketDeYama,tipoMensaje,string_length(contenidoArchivo),contenidoArchivo);
	free(contenidoArchivo);
}
*/
void verificarNodo(char* nodo){
    uint32_t cantidadNodos = list_size(nombresNodos);
    uint32_t posicion;
    uint32_t corte = 0;

    if(cantidadNodos == 0){
        list_add(nombresNodos,nodo);
    }

    for(posicion=0; posicion<cantidadNodos; posicion++){
        if(corte==0){
          char* nodoAux = list_get(nombresNodos,posicion);
          if(strcmp(nodoAux,nodo) != 0){
              list_add(nombresNodos,nodo);
              corte++;
          }
      }
    }
}

uint32_t tamanioArchivoAModificar(){
    return string_length(archivoAModificar) + sizeof(uint32_t);
}

void* serializarArchivoAModificar(){
    uint32_t posicionActual = 0;

    void* datosSerializados = malloc(tamanioArchivoAModificar());

    uint32_t tamanioArchivo = string_length(archivoAModificar);
    memcpy(datosSerializados + posicionActual, &tamanioArchivo, sizeof(uint32_t));
    posicionActual += sizeof(uint32_t);
    memcpy(datosSerializados + posicionActual, &archivoAModificar, tamanioArchivo);
    posicionActual += tamanioArchivo;

    return datosSerializados;
}

int recvDeNotificacionMaster(int deQuien){
    int notificacion;
    int resultado;
    if((resultado = recv(deQuien, &notificacion, sizeof(int), 0))==-1){
        perror("Error al recibir la notificacion.");
        exit(-1);
    }

    if(resultado==0){
        return 0;
    } else{
        return notificacion;
    }
}


int conectarAWorker(char *ip, int puerto) { //Recibe ip y puerto, devuelve socket que se conecto

    int socket_server = socket(AF_INET, SOCK_STREAM, 0);
    struct hostent *infoDelServer;
    struct sockaddr_in direccion_server; // información del server

    //Obtengo info del server
    if ((infoDelServer = gethostbyname(ip)) == NULL) {
        perror("Error al obtener datos del server.");
    }

    //Guardo datos del server
    direccion_server.sin_family = AF_INET;
    direccion_server.sin_port = htons(puerto);
    direccion_server.sin_addr = *(struct in_addr *) infoDelServer->h_addr; //h_addr apunta al primer elemento h_addr_lista
    memset(&(direccion_server.sin_zero), 0, 8);

    //Conecto con servidor, si hay error finalizo
    if (connect(socket_server, (struct sockaddr *) &direccion_server,sizeof(struct sockaddr)) == -1) {
        perror("Error al conectar con el servidor.");
    }

    return socket_server;

}
