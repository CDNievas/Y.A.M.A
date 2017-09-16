#include "configParser.h"


// Imprime archivo de configuracion por pantalla
void chequearParametros(int argc, int cantidad){
    if(argc != cantidad){
        perror("Error con los parametros. \n");
        exit(-1);
    }
}

void chequearExistenciaArchivo(char* pathConfiguracion){
    if(!existeArchivo(pathConfiguracion)){
        perror("El archivo de configuracion no existe. \n");
        exit(-1);
    }
}

void chequearCantidadDeKeys(t_config* configuracion, int cantidadKeys){
    if(cantidadKeys != config_keys_amount(configuracion)){
        perror("Faltan datos en el archivo de configuracion. \n");
        exit(-1);
    }
}

t_config* generarTConfig(char* pathConfiguracion, int cantidadKeys){
    t_config* configuracion;
    chequearExistenciaArchivo(pathConfiguracion);
    if((configuracion = config_create(pathConfiguracion))==NULL){
        perror("Error al generar t_config. \n");
        exit(-1);
    }
    chequearCantidadDeKeys(configuracion, cantidadKeys);
    return configuracion;
}
