/*
 ============================================================================
 Name        : YAMA.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "reduccionLocal.h"
#include "transformacion.h"
#include "reduccionGlobal.h"
#include "funcionesYAMA.h"
#include "../../Biblioteca/src/Socket.c"
#include "../../Biblioteca/src/configParser.c"

void manejadorMaster(void* socketMasterCliente){
	int socketMaster = *(int*)socketMasterCliente;
	int sigueProcesando = 1;
	char* nombreArchivoPeticion;
	char* nodoFallido;
	uint32_t nroMaster = obtenerNumeroDeMaster();
	log_info(loggerYAMA, "El numero de master del socket %d es %d.", socketMaster, nroMaster);
	bool pudoReplanificar = 1;
	while(sigueProcesando && estaFS){ //USAR BOOLEAN PARA CORTAR CUANDO TERMINE LA OPERACION Y MATAR EL HILO
		int operacionMaster = recvDeNotificacion(socketMaster);
		switch (operacionMaster){
			case TRANSFORMACION:
				log_trace(loggerYAMA, "Se recibio una peticion del socket %d para llevar a cabo una transformacion.", socketMaster);
				nombreArchivoPeticion = recibirNombreArchivo(socketMaster); //RECIBO TAMANIO DE NOMBRE Y NOMBRE
				solicitarArchivo(nombreArchivoPeticion);
				log_info(loggerYAMA, "Se envio la peticion del archivo a FileSystem.");
				t_list* listaDeBloquesDeArchivo = recibirInfoArchivo(); //RECIBO LOS DATOS DE LOS BLOQUES (CADA CHAR* CON SU LONGITUD ANTES)
				if(listaDeBloquesDeArchivo != NULL && estaFS){
					log_debug(loggerYAMA, "Se recibieron los datos del archivo de FileSystem.");
					log_trace(loggerYAMA, "Preparando los datos para el balanceo de cargas.");
					t_list* listaBalanceo = armarDatosBalanceo(listaDeBloquesDeArchivo);
					log_info(loggerYAMA, "Llevando a cabo el balanceo de cargas con el algoritmo %s.", ALGORITMO_BALANCEO);
					t_list* listaDeCopias = balancearTransformacion(listaDeBloquesDeArchivo, listaBalanceo);
					log_info(loggerYAMA, "Se prosigue a cargar la transformacion en la tabla de estados.");
					if(cargarTransformacion(socketMaster, nroMaster, listaDeBloquesDeArchivo, listaDeCopias) != 1){
						estaFS = false;
						log_error(loggerYAMA, "No se pueden llevar a cabo las peticiones del master %d debido a problemas en la conexion con FileSystem.", nroMaster);
						log_error(loggerYAMA, "Liberando recursos del hilo del master %d.", nroMaster);
					}
					list_destroy(listaDeBloquesDeArchivo); //LEAKS AQUI (si hay...)
					list_destroy(listaDeCopias);
					list_destroy_and_destroy_elements(listaBalanceo, (void*)liberarDatosBalanceo);
				}else if(listaDeBloquesDeArchivo == NULL && estaFS){
					log_error(loggerYAMA, "El archivo %s solicitado por el master %d no existe en el FileSystem.", nombreArchivoPeticion, nroMaster);
					sigueProcesando = 0;
				}
				break;
			case TRANSFORMACION_TERMINADA:
				log_trace(loggerYAMA, "Se recibio una peticion del master %d para finalizar una transformacion.", nroMaster);
				char* nombreNodo = recibirString(socketMaster);
				terminarTransformacion(nroMaster, socketMaster, nombreNodo); //RECIBO TAMANIO NOMBRE NODO, NODO, NRO DE BLOQUE
				t_list* listaDelJob = obtenerListaDelNodo(nroMaster, socketMaster, nombreNodo);
				log_trace(loggerYAMA, "Se prosigue a chequear si se puede llevar a cabo la reduccion local en el nodo %s.", nombreNodo);
				if(sePuedeHacerReduccionLocal(listaDelJob)){
					log_debug(loggerYAMA, "Se puede llevar a cabo la reduccion local.");
					if(cargarReduccionLocal(socketMaster, nroMaster, listaDelJob) != 1){
						estaFS = false;
					}
				}else{
					log_warning(loggerYAMA, "No se puede llevar a cabo la reduccion local aun.");
					sendDeNotificacion(socketMaster, NO_REDU_LOCAL);
				}
				free(nombreNodo);
				list_destroy(listaDelJob);
				break;
			case REPLANIFICAR:
				nodoFallido = recibirString(socketMaster); //RECIBO NOMBRE DEL NODO A REPLANIFICAR
				log_warning(loggerYAMA, "El nodo %s fallo en su etapa de transformacion.", nodoFallido);
				log_trace(loggerYAMA, "Se prosigue a replanificar los bloques del nodo %s.", nodoFallido);
				cargarFallo(nroMaster, nodoFallido); //MODIFICO LA TABLA DE ESTADOS (PONGO EN FALLO LAS ENTRADAS DEL NODO)
				solicitarArchivo(nombreArchivoPeticion); // PIDO NUEVAMENTE LOS DATOS A FS
				t_list* listaDeBloques = recibirInfoArchivo(); // RECIBO LOS DATOS DEL ARCHIVO
				if(listaDeBloques != NULL){
					pudoReplanificar = cargarReplanificacion(socketMaster, nroMaster, nodoFallido, listaDeBloques); // CARGO LA REPLANIFICACION EN LA TABLA DE ESTADOS (ACA REPLANIFICO)
					list_destroy(listaDeBloques);
					if(pudoReplanificar == 0){
						sendDeNotificacion(socketMaster, ABORTAR);
						pthread_cancel(pthread_self());
					}else if(pudoReplanificar == -1){
						estaFS = false;
					}
				}else{
					estaFS = false;
				}
				free(nodoFallido);
				break;
			case REDUCCION_LOCAL_TERMINADA:
				log_trace(loggerYAMA, "Se recibio una notificacion para finalizar con una reduccion local por parte del master %d.", nroMaster);
				terminarReduccionLocal(nroMaster, socketMaster); //RECIBO NOMBRE NODO VA A HABER UNA UNICA INSTANCIA DE NODO HACIENDO REDUCCION LOCAL
				log_trace(loggerYAMA, "Se prosigue a chequear si se puede llevar a cabo la reduccion global en el master %d.", nroMaster);
				if(sePuedeHacerReduccionGlobal(nroMaster)){ //CHEQUEO SI TODOS LOS NODOS TERMINARON DE REDUCIR
					log_debug(loggerYAMA, "Se puede llevar a cabo la reduccion global en el master %d.", nroMaster);
					t_list* bloquesReducidos = filtrarReduccionesDelNodo(nroMaster); //OBTENGO TODAS LAS REDUCCIONES LOCALES QUE HIZO EL MASTER
					if(cargarReduccionGlobal(socketMaster, nroMaster, bloquesReducidos) != 1){
						estaFS = false;
					}
					list_destroy(bloquesReducidos);
				}else{
					log_info(loggerYAMA, "Todavia no se puede llevar a cabo la reduccion global en el master %d.", nroMaster);
				}
				break;
			case REDUCCION_GLOBAL_TERMINADA:
				log_trace(loggerYAMA, "Se recibio una notificacion para finalizar la reduccion global por parte del master %d.", nroMaster);
				terminarReduccionGlobal(nroMaster);
				log_trace(loggerYAMA, "Se prosigue a realizar el almacenado final para el master %d.", nroMaster);
				if(almacenadoFinal(socketMaster, nroMaster) != 1){
					estaFS = false;
				}
				break;
			case FINALIZO:
				reestablecerWL(nroMaster);
				log_debug(loggerYAMA, "El Master %d termino su Job.\nTerminando su ejecucioin.\nCerrando la conexion.", nroMaster);
				sigueProcesando = 0;
				break;
			case ERROR_REDUCCION_LOCAL:
				fallaReduccionLocal(nroMaster);
				log_warning(loggerYAMA, "Error de reduccion local.");
				log_warning(loggerYAMA,"Abortando el Job.");
				sendDeNotificacion(socketMaster, ABORTAR);
				sigueProcesando = 0;
				break;
			case ERROR_REDUCCION_GLOBAL:
				fallaReduccionGlobal(nroMaster);
				log_warning(loggerYAMA, "Error de reduccion global.");
				log_warning(loggerYAMA,"Abortando el Job.");
				sendDeNotificacion(socketMaster, ABORTAR);
				sigueProcesando = 0;
				break;
			case ERROR_ALMACENAMIENTO_FINAL:
				log_warning(loggerYAMA, "El master %d fallo a la hora de llevar a cabo el almacenamiento final.");
				log_warning(loggerYAMA, "Abortando Job.");
				sendDeNotificacion(socketMaster, ABORTAR);
				sigueProcesando = 0;
				break;
			case CORTO:
				log_warning(loggerYAMA, "El master %d corto.", nroMaster);
				sigueProcesando = 0;
				break;
			default:
				log_error(loggerYAMA, "La peticion recibida por el master %d es erronea.", socketMaster);
				sendDeNotificacion(socketMaster, ABORTAR);
				sigueProcesando = 0;
				break;
		}
	}
	if(!estaFS){
		sendDeNotificacion(socketMaster, ABORTAR);
		log_info(loggerYAMA, "Se le informo al master %d del error de conexion.", nroMaster);
		log_info(loggerYAMA, "Se cerro el hilo del master %d.", nroMaster);
		log_info(loggerYAMA, "El FileSystem corto la conexion.");
		log_info(loggerYAMA, "Se cierra YAMA.");
		close(socketMaster);
		pthread_cancel(pthread_self());
//		usleep(20000);
		exit(0);
	}
	if(!sigueProcesando){
		log_info(loggerYAMA, "Se le informo al master %d que debe cortar la conexion.", nroMaster);
		free(nombreArchivoPeticion);
		close(socketMaster);
		pthread_cancel(pthread_self());
	}
}

int main(int argc, char *argv[])
{
	signal(SIGUSR1, chequeameLaSignal);
	signal(SIGINT, laParca);
	loggerYAMA = log_create("YAMA.log", "YAMA", 1, 0);
	chequearParametros(argc,2);
	t_config* configuracionYAMA = generarTConfig(argv[1], 6);
//	t_config* configuracionYAMA = generarTConfig("Debug/off_yama.ini", 6);
	cargarYAMA(configuracionYAMA);
	log_debug(loggerYAMA, "Se cargo exitosamente YAMA.");
	nodosSistema = list_create();
	socketFS = conectarAServer(FS_IP, FS_PUERTO);
	log_debug(loggerYAMA, "Conexion con FileSystem realizada.");
	handshakeFS();
	estaFS = true;
	log_info(loggerYAMA, "Handshake con FileSystem realizado.");
	socketEscuchaMasters = ponerseAEscucharClientes(PUERTO_MASTERS, 0);
 	log_info(loggerYAMA, "Escuchando clientes...");
	int socketMaximo = socketEscuchaMasters, socketClienteChequeado, socketAceptado;
	fd_set socketsMasterCPeticion, socketMastersAuxiliares;
	FD_ZERO(&socketMastersAuxiliares);
	FD_ZERO(&socketsMasterCPeticion);
	FD_SET(socketEscuchaMasters, &socketsMasterCPeticion);
 	//HILOS
	pthread_t hiloManejadorMaster;
 	pthread_attr_t attr;
 	pthread_attr_init(&attr);
 	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
  	tablaDeEstados = list_create();
  	//FIN HILOS
  	//INICIALIZO SEMAFOROS PARA VAR COMPARTIDAS.
  	pthread_mutex_init(&semNodosSistema, NULL);
  	pthread_mutex_init(&semTablaEstados, NULL);
  	pthread_mutex_init(&semContMaster, NULL);
  	pthread_mutex_init(&semContJobs, NULL);
  	pthread_mutex_init(&semReducGlobales, NULL);
  	pthread_mutex_init(&semReducLocales, NULL);
  	pthread_mutex_init(&semTransformaciones, NULL);
  	//FIN DE SEMAFOROS

	while(estaFS){
		socketMastersAuxiliares = socketsMasterCPeticion;
		int selectVal = select(socketMaximo+1, &socketMastersAuxiliares, NULL, NULL, NULL);
		if(selectVal<0){
			if(selectVal != EINTR){
				log_error(loggerYAMA, "Fallo el select.");
				exit(-1);
			}
		}
		log_info(loggerYAMA, "Un socket realizo una peticion a YAMA.");
		for(socketClienteChequeado = 0; socketClienteChequeado <= socketMaximo; socketClienteChequeado++){
			if(FD_ISSET(socketClienteChequeado, &socketMastersAuxiliares)){
				if(socketClienteChequeado == socketEscuchaMasters){
					socketAceptado = aceptarConexionDeCliente(socketEscuchaMasters);
					FD_SET(socketAceptado, &socketsMasterCPeticion);
					socketMaximo = calcularSocketMaximo(socketAceptado, socketMaximo);
					log_info(loggerYAMA, "Se recibio una nueva conexion del socket %d.", socketAceptado);
				}else{
					int notificacion = recvDeNotificacion(socketClienteChequeado);
					if(notificacion != ES_MASTER){
						log_error(loggerYAMA, "La conexion del socket %d es erronea.", socketClienteChequeado);
						FD_CLR(socketClienteChequeado, &socketsMasterCPeticion);
						close(socketClienteChequeado);
					}else{
						sendDeNotificacion(socketClienteChequeado, ES_YAMA);
		        		log_trace(loggerYAMA, "Se establecio la conexion con el socket master %d - Handshake realizado.", socketClienteChequeado);
		       			int* socketCliente = malloc(sizeof(int));
		            	*socketCliente = socketClienteChequeado;
		            	pthread_create(&hiloManejadorMaster, &attr, (void*)manejadorMaster, (void*)socketCliente);
		            	log_trace(loggerYAMA, "Pasando a atender la peticion del socket master %d.", socketClienteChequeado);
		           		FD_CLR(socketClienteChequeado, &socketsMasterCPeticion);
					}
				}
			}
		}
	}
	return EXIT_SUCCESS;
}
