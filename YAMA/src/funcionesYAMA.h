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
void liberarConexion(conexionNodo*);
void liberarInfoFS(infoDeFs*);

t_list* obtenerListaDelNodo(int, int);
char* obtenerNombreNodo(t_list*);
int obtenerJobDeNodo(t_list*);
void cargarYAMA(t_config*);

//SIGNAL
void chequeameLaSignal(int);
/*
 * ENVIO LA NUEVA COPIA A MASTER TRAS LA REPLANIFICACION
 */
void enviarCopiaAMaster(int, copia*);
//HANDSHAKE CON FS
void handshakeFS();
//FUNCIONES PARA ADMINISTRACION DE BALANCEO
int obtenerWLMax();
int calculoAvailability(char*);
t_list* armarDatosBalanceo(t_list*);

//BUSCO EL NODO ENCARGADO DE LA REDUCCION GLOBAL
char* buscarNodoEncargado(uint32_t);

t_list* filtrarTablaMaster(uint32_t);
#endif /* FUNCIONESYAMA_H_ */
