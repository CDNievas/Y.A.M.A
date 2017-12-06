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
	free(socketMasterCliente);
	char* nombreArchivoPeticion;
	char* nodoFallido;
	uint32_t nroMaster = obtenerNumeroDeMaster();
	log_trace(loggerYAMA, "SOCKET %d - MASTER %d", socketMaster, nroMaster);
	bool pudoReplanificar = 1;
	while(sigueProcesando && estaFS){ //USAR BOOLEAN PARA CORTAR CUANDO TERMINE LA OPERACION Y MATAR EL HILO
		int operacionMaster = recvDeNotificacion(socketMaster);
		switch (operacionMaster){
			case TRANSFORMACION:
				log_debug(loggerYAMA, "TRANSFORMACION - MASTER %d", nroMaster);
				nombreArchivoPeticion = recibirNombreArchivo(socketMaster); //RECIBO TAMANIO DE NOMBRE Y NOMBRE
				solicitarArchivo(nombreArchivoPeticion);
				log_info(loggerYAMA, "PETICION INFORMACION DE ARCHIVO - ARCHIVO %s", nombreArchivoPeticion);
				t_list* listaDeBloquesDeArchivo = recibirInfoArchivo(); //RECIBO LOS DATOS DE LOS BLOQUES (CADA CHAR* CON SU LONGITUD ANTES)
				if(listaDeBloquesDeArchivo != NULL && estaFS){
					log_debug(loggerYAMA, "INFORMACION DE ARCHIVO RECIBIDA");
					log_trace(loggerYAMA, "PREPARANDO BALANCEO DE CARGAS - ALGORITMO %s", ALGORITMO_BALANCEO);
					t_list* listaBalanceo = armarDatosBalanceo(listaDeBloquesDeArchivo);
					t_list* listaDeCopias = balancearTransformacion(listaDeBloquesDeArchivo, listaBalanceo);
					log_info(loggerYAMA, "REGISTRANDO TRANSFORMACION");
					if(cargarTransformacion(socketMaster, nroMaster, listaDeBloquesDeArchivo, listaDeCopias) != 1){
						estaFS = false;
						log_error(loggerYAMA, "ERROR - PROBLEMAS DE CONEXION CON FILESYSTEM");
					}
					list_destroy_and_destroy_elements(listaDeBloquesDeArchivo, (void*)liberarInfoFS); //LEAKS AQUI (si hay...)
					list_destroy(listaDeCopias);
					list_destroy_and_destroy_elements(listaBalanceo, (void*)liberarDatosBalanceo);
				}else if(listaDeBloquesDeArchivo == NULL && estaFS){
					log_error(loggerYAMA, "ARCHIVO %s INEXISTENTE - MASTER %d", nombreArchivoPeticion, nroMaster);
					sigueProcesando = 0;
				}
				break;
			case TRANSFORMACION_TERMINADA:{
				char* nombreNodo = recibirString(socketMaster);
				terminarTransformacion(nroMaster, socketMaster, nombreNodo); //RECIBO TAMANIO NOMBRE NODO, NODO, NRO DE BLOQUE
				t_list* listaDelJob = obtenerListaDelNodo(nroMaster, socketMaster, nombreNodo);
				log_debug(loggerYAMA, "TRANSFORMACION TERMINADA - MASTER %d - NODO %s.", nroMaster, nombreNodo);
				if(sePuedeHacerReduccionLocal(listaDelJob)){
					log_debug(loggerYAMA, "REDUCCION LOCAL - MASTER %d - NODO %s", nroMaster, nombreNodo);
					if(cargarReduccionLocal(socketMaster, nroMaster, listaDelJob) != 1){
						estaFS = false;
						log_error(loggerYAMA, "ERROR - PROBLEMAS DE CONEXION CON FILESYSTEM");
					}
				}else{
					log_warning(loggerYAMA, "REDUCCION LOCAL NO PERMITIDA - NODO %s", nombreNodo);
					sendDeNotificacion(socketMaster, NO_REDU_LOCAL);
				}
				free(nombreNodo);
				list_destroy(listaDelJob);
			}
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
					/*list_destroy(listaDeBloques);*/
					if(pudoReplanificar == 0){
						sendDeNotificacion(socketMaster, ABORTAR);
						sigueProcesando = false;
					}else if(pudoReplanificar == -1){
						estaFS = false;
						log_error(loggerYAMA, "ERROR - PROBLEMAS DE CONEXION CON FILESYSTEM");
					}
				}else{
					estaFS = false;
				}
				list_destroy_and_destroy_elements(listaDeBloques, (void*)liberarInfoFS);
				free(nodoFallido);
				break;
			case REDUCCION_LOCAL_TERMINADA:
				terminarReduccionLocal(nroMaster, socketMaster); //RECIBO NOMBRE NODO VA A HABER UNA UNICA INSTANCIA DE NODO HACIENDO REDUCCION LOCAL
				if(sePuedeHacerReduccionGlobal(nroMaster)){ //CHEQUEO SI TODOS LOS NODOS TERMINARON DE REDUCIR
					log_debug(loggerYAMA, "REDUCCION GLOBAL - MASTER %d", nroMaster);
					t_list* bloquesReducidos = filtrarReduccionesDelNodo(nroMaster); //OBTENGO TODAS LAS REDUCCIONES LOCALES QUE HIZO EL MASTER
					if(cargarReduccionGlobal(socketMaster, nroMaster, bloquesReducidos) != 1){
						estaFS = false;
						log_error(loggerYAMA, "ERROR - PROBLEMAS DE CONEXION CON FILESYSTEM");
					}
					list_destroy(bloquesReducidos);
				}else{
					log_info(loggerYAMA, "REDUCCION GLOBAL NO PERMITIDA - MASTER %d", nroMaster);
				}
				break;
			case REDUCCION_GLOBAL_TERMINADA:
				log_debug(loggerYAMA, "REDUCCION GLOBAL TERMINADA - MASTER %d", nroMaster);
				terminarReduccionGlobal(nroMaster);
				log_debug(loggerYAMA, "ALMACENAMIENTO FINAL - MASTER %d", nroMaster);
				if(almacenadoFinal(socketMaster, nroMaster) != 1){
					estaFS = false;
				}
				break;
			case FINALIZO:
				reestablecerWL(nroMaster);
				log_debug(loggerYAMA, "FINALIZACION DE JOB - MASTER %d", nroMaster);
				sigueProcesando = 0;
				break;
			case ERROR_REDUCCION_LOCAL:
				fallaReduccionLocal(nroMaster);
				log_warning(loggerYAMA, "ERROR - REDUCCION LOCAL - MASTER %d", nroMaster);
				log_warning(loggerYAMA,"ABORTANDO - MASTER %d", nroMaster);
				sendDeNotificacion(socketMaster, ABORTAR);
				sigueProcesando = 0;
				break;
			case ERROR_REDUCCION_GLOBAL:
				fallaReduccionGlobal(nroMaster);
				log_warning(loggerYAMA, "ERROR - REDUCCION GLOBAL - MASTER %d", nroMaster);
				log_warning(loggerYAMA,"ABORTANDO - MASTER %d", nroMaster);
				sendDeNotificacion(socketMaster, ABORTAR);
				sigueProcesando = 0;
				break;
			case ERROR_ALMACENAMIENTO_FINAL:
				log_warning(loggerYAMA, "ERROR - ALMACENAMIENTO FINAL - MASTER %d", nroMaster);
				log_warning(loggerYAMA, "ABORTANDO - MASTER %d", nroMaster);
				sendDeNotificacion(socketMaster, ABORTAR);
				sigueProcesando = 0;
				break;
			case CORTO:
				log_warning(loggerYAMA, "CORTO - MASTER %d", nroMaster);
				sigueProcesando = 0;
				break;
			default:
				log_error(loggerYAMA, "PETICION ERRONEA - MASTER %", nroMaster);
				log_warning(loggerYAMA, "ABORTANDO - MASTER %d", nroMaster);
				sendDeNotificacion(socketMaster, ABORTAR);
				sigueProcesando = 0;
				break;
		}
	}
	if(!estaFS){
		sendDeNotificacion(socketMaster, ABORTAR);
		log_error(loggerYAMA, "ERROR - CORTO FILESYSTEM");
		log_warning(loggerYAMA, "MUERE HILO - MASTER %d", nroMaster);
		log_warning(loggerYAMA, "CIERRA YAMA");
		free(nombreArchivoPeticion);
		close(socketMaster);
		pthread_cancel(pthread_self());
//		usleep(20000);
		exit(0);
	}
	if(!sigueProcesando){
		log_info(loggerYAMA, "CORTA CONEXION - MASTER %d", nroMaster);
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
	imprimirConfigs();
	log_debug(loggerYAMA, "CARGA EXITOSA DE YAMA");
	nodosSistema = list_create();
	socketFS = conectarAServer(FS_IP, FS_PUERTO);
	log_debug(loggerYAMA, "CONEXION CON FILESYSTEM");
	handshakeFS();
	estaFS = true;
	log_debug(loggerYAMA, "HANDSHAKE CON FILESYSTEM");
	socketEscuchaMasters = ponerseAEscucharClientes(PUERTO_MASTERS, 0);
 	log_info(loggerYAMA, "SOCKET ESCUCHA ACTIVADO");
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
				log_error(loggerYAMA, "ERROR - FALLO SELECT");
				log_error(loggerYAMA, "ERROR - MUERE YAMA");
				exit(-1);
			}
		}
		log_info(loggerYAMA, "PETICION DE SOCKET");
		for(socketClienteChequeado = 0; socketClienteChequeado <= socketMaximo; socketClienteChequeado++){
			if(FD_ISSET(socketClienteChequeado, &socketMastersAuxiliares)){
				if(socketClienteChequeado == socketEscuchaMasters){
					socketAceptado = aceptarConexionDeCliente(socketEscuchaMasters);
					FD_SET(socketAceptado, &socketsMasterCPeticion);
					socketMaximo = calcularSocketMaximo(socketAceptado, socketMaximo);
					log_info(loggerYAMA, "NUEVA CONEXION - SOCKET %d", socketAceptado);
				}else{
					int notificacion = recvDeNotificacion(socketClienteChequeado);
					if(notificacion != ES_MASTER){
						log_error(loggerYAMA, "CONEXION ERRONEA - SOCKET %d", socketClienteChequeado);
						FD_CLR(socketClienteChequeado, &socketsMasterCPeticion);
						close(socketClienteChequeado);
					}else{
						sendDeNotificacion(socketClienteChequeado, ES_YAMA);
		        		log_trace(loggerYAMA, "MASTER %d - HANDSHAKE REALIZADO", socketClienteChequeado);
		       			int* socketCliente = malloc(sizeof(int));
		            	*socketCliente = socketClienteChequeado;
		            	pthread_create(&hiloManejadorMaster, &attr, (void*)manejadorMaster, (void*)socketCliente);
		           		FD_CLR(socketClienteChequeado, &socketsMasterCPeticion);
					}
				}
			}
		}
	}
	return EXIT_SUCCESS;
}
