/*
 * funcionesYAMA.h
 *
 *  Created on: 22/9/2017
 *      Author: utnso
 */
#include "estructuras.h"
#include "serializaciones.h"

#ifndef FUNCIONESYAMA_H_
#define FUNCIONESYAMA_H_

//GENERACION DE ESTRUCTURAS
administracionYAMA* generarAdministracion(uint32_t, uint32_t, uint32_t, char*);
conexionNodo* generarConexionNodo();
infoDeFs* generarInformacionDeBloque();
nodoSistema* generarNodoSistema();

//GENERO NOMBRE RANDOM
char* obtenerNombreTemporalLocal();
char* obtenerNombreTemporalGlobal();
char* obtenerNombreTemporalTransformacion();
int obtenerNumeroDeJob();
int obtenerNumeroDeMaster();

//OBTENGO DATOS DE CONEXION
void obtenerIPYPuerto(conexionNodo*);

//LIBERACION DE DATOS
void liberarDatoMaster(infoNodo*);
void liberarConexion(conexionNodo*);
void liberarInfoFS(infoDeFs*);
void liberarDatosBalanceo(datosBalanceo*);
void liberarNodoSistema(nodoSistema*);
void liberarCopia(copia*);
void liberarInfoNodo(infoNodo*);

t_list* obtenerListaDelNodo(int, int, char*);
char* obtenerNombreNodo(t_list*);
int obtenerJobDeNodo(t_list*);
void cargarYAMA(t_config*);

//SIGNAL
void chequeameLaSignal(int);
void laParca(int);

void imprimirConfigs();
void imprimirWLs();
/*
 * ENVIO LA NUEVA COPIA A MASTER TRAS LA REPLANIFICACION
 */
void enviarCopiaAMaster(int, copia*);
//HANDSHAKE CON FS
void handshakeFS();
//FUNCIONES PARA ADMINISTRACION DE BALANCEO
uint32_t obtenerWLMax();
int calculoAvailability(char*);
t_list* armarDatosBalanceo(t_list*);

//BUSCO EL NODO ENCARGADO DE LA REDUCCION GLOBAL
char* buscarNodoEncargado(uint32_t);

t_list* filtrarTablaMaster(uint32_t);

uint32_t peekingNotificacion(int);
#endif /* FUNCIONESYAMA_H_ */
