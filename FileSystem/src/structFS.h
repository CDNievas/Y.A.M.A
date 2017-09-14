#include "../../Biblioteca/src/genericas.c"
#include "../../Biblioteca/src/configParser.c"
#include "../../Biblioteca/src/Socket.c"
#include <readline/history.h>
#include <readline/chardefs.h>

#ifndef STRUCTFS_H_
#define STRUCTFS_H_

typedef struct t_directory {
	int index;
	char nombre[255];
	int padre;
} directorio;


#endif /* STRUCTFS_H_ */
