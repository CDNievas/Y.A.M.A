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
