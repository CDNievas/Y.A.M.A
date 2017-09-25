#include "balanceoDeCargas.h"


void liberarPosible(posibleElegido* posible){
	free(posible->nombreNodo);
	free(posible);
}

int generarPrimerPosible(t_list* listaDelJob, infoDeFs* info){
	bool esDelPrimerNodo(administracionYAMA* admin){
		return strcmp(info->copia1->nombreNodo, admin->nombreNodo);
	}
	int primerPosible = list_count_satisfying(listaDelJob, (void*)esDelPrimerNodo);
	return primerPosible;
}

int generarSegundoPosible(t_list* listaDelJob, infoDeFs* info){
	bool esDelSegundoNodo(administracionYAMA* admin){
		return strcmp(info->copia1->nombreNodo, admin->nombreNodo);
	}
	int segundoPosible = list_count_satisfying(listaDelJob, (void*)esDelSegundoNodo);
	return segundoPosible;
}

copia* balancearCarga(int nroJob, infoDeFs* info){
	bool perteneceAlJob(administracionYAMA* admin){
			return admin->nroJob == nroJob;
	}
	int primerPosible;
	int segundoPosible;
	if(strcmp(ALGORITMO_BALANCEO, "RR")){
		t_list* listaDeJob = list_filter(tablaDeEstados, (void*)perteneceAlJob);
		primerPosible = generarPrimerPosible(listaDeJob, info);
		segundoPosible = generarSegundoPosible(listaDeJob, info);
		free(listaDeJob);
	}else if(strcmp(ALGORITMO_BALANCEO, "WRR")){
		primerPosible = generarPrimerPosible(tablaDeEstados, info);
		segundoPosible = generarSegundoPosible(tablaDeEstados, info);
	}else{
		log_error(loggerYAMA, "Error en el algoritmo de balanceo.\nChequear archivo de configuracion.");
		return NULL;
	}
	if(primerPosible <= segundoPosible){
		return info->copia1;
	}else{
		return info->copia2;
	}
}

char* nodoParaReduccionGlobal(int nroJob){

	//PARA RR: FILTRO LAS OPERACIONES DEL JOB Y ME FIJO CUAL ES EL QUE MENOS TIENE
	//PARA WRR: FILTRO OPERACIONES DEL JOB Y A PARTIR DE LOS NODOS VOY CONTANDO EN TODA LA TABLA
	return NULL;
}

//-----------------------------------REPLANIFICACION-------------------------------------------//
//--------------------------------FUNCIONES AUXILIARES DE PLANIFICACION------------------------//
copia* obtenerLaOtraCopia(infoDeFs* info, copia* copiaFallida){
	if(strcmp(info->copia1->nombreNodo, copiaFallida->nombreNodo)){
		return info->copia2;
	}else if(strcmp(info->copia2->nombreNodo, copiaFallida->nombreNodo)){
		return info->copia1;
	}else{
		log_error(loggerYAMA, "Error al tratar de obtener la otra copia del archivo durante replanificacion.\nABORTANDO.");
		exit(-1);
	}
}
copia* obtenerOtraCopia(t_list* listaDeArchivo, copia* copiaFallida){
	bool esDeLaCopia(infoDeFs* info){
		return (strcmp(info->copia1->nombreNodo, copiaFallida->nombreNodo) && info->copia1->nroBloque == copiaFallida->nroBloque) || (strcmp(info->copia2->nombreNodo, copiaFallida->nombreNodo)&& info->copia2->nroBloque == copiaFallida->nroBloque); //EL MEJOR IF DE MI VIDA XD
	}
	infoDeFs* info = list_find(listaDeArchivo, (void*)esDeLaCopia);
	return obtenerLaOtraCopia(info, copiaFallida);
}

copia* replanificarTransformacion(int nroMaster, t_list* listaDeArchivo , void* mensaje){
	copia* copiaFallida = deserializarCopia(mensaje);
	free(copiaFallida->nombreNodo);
	free(copiaFallida);
	return obtenerOtraCopia(listaDeArchivo, copiaFallida);
}
