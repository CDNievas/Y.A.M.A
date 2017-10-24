#include "genericas.h"

void * miMalloc(size_t size, t_log * logger, char * msg){

	void * p;
	if((p = malloc(size)) == NULL){
		log_error(logger,"Error de malloc(). %s",msg);
		exit(-10);
	}

	return p;

}
