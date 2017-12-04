#include "Consola.h"


uint32_t countAmmount = 12;

typedef struct {
	int flag;
	char *nombre;     /* Nombre de la funcion */
} command;


command commands[] = {
	{ 1,"format"},
	{ 2,"rm"},
	{ 3,"rename"},
	{ 4,"mv"},
	{ 5,"cat"},
	{ 6,"mkdir"},
	{ 7,"cpfrom"},
	{ 8,"cpto"},
	{ 9,"cpblock"},
	{ 10,"md5"},
	{ 11,"ls"},
	{ 12,"info"}
};


void imprimirComandos(){
	puts("\n");
	puts("*************************************************\n");
	puts("*                                               *\n");
	puts("*   Elegir para ejecutar dicho comando:         *\n");
	puts("*                                               *\n");
	puts("*************************************************\n");
	puts("\n");

	puts("format: Formatear el FileSystem. \n");
	puts("rm: Eliminar archivo/directorio/nodo/bloque. \n");
	puts("rename: Renombrar un archivo o directorio. \n");
	puts("mv: Mover un archivo o directorio. \n");
	puts("cat: Mostrar el contenido del archivo como texto plano. \n");
	puts("mkdir: Crear un directorio. \n");
	puts("cpfrom: Copiar un archivo local al yamafs. \n");
	puts("cpto: Copiar un archivo local al yamafs. \n");
	puts("cpblock: Crear copia de un bloque de un archivo. \n");
	puts("md5: Solicitar MD5 de un archivo del yamafs. \n");
	puts("ls Listar los archivos de un directorio. \n");
	puts("info: Mostrar información de un archivo. \n");
}

void analizeCommand(char* cmd){
	char** disarmedCmd = string_split(cmd, " ");
	char* function = disarmedCmd[0];
	uint32_t commandNumber;
	command auxCommand;
	int count;
	for(count = 0; count < countAmmount; count++){
		auxCommand = commands[count];
		if(strcmp(function, auxCommand.nombre)==0){
			commandNumber = auxCommand.flag;
			break;
		}
	}

	switch (commandNumber) {
    case 1:
    	if(hayEstadoAnterior==false){
    		uint cantidadNodosSistemas=list_size(tablaGlobalNodos->nodo);
    		if(cantidadNodosSistemas>=2){
    			estaFormateado=true;
    			esEstadoSeguro=true;
    		}else{
    			log_error(loggerFileSystem,"No hay suficientes DataNode para dejar el FS en un estado Estable");
    		}
    	} else {
    		if(hayUnEstadoEstable()){
    			esEstadoSeguro=true;
    		} else {
    			log_error(loggerFileSystem,"No hay al menos una copia de cada archivo. Estado no estable.");
    		}
    		log_info(loggerFileSystem, "FileSystem formateado.");
    	}
    		break;
    case 2:
    	if(strcmp(disarmedCmd[1], "-d") == 0){
    		borrarDirectorio(disarmedCmd[2]);
    	} else if(strcmp(disarmedCmd[1], "-b") == 0){
    	} else {
    		borrarArchivo(disarmedCmd[1]);
    	}
    break;
    case 3:{
    	char * nombreArchivoViejo = disarmedCmd[1];
    	char * nombreArchivoNuevo = disarmedCmd[2];
    	if(nombreArchivoViejo == NULL || nombreArchivoNuevo == NULL){
    		log_error(loggerFileSystem, "Faltan parametros para ejecutar el comando rename");
    	} else {
    		renameDirectory(nombreArchivoViejo,nombreArchivoNuevo);
    	}
    }
    break;
    case 4:{
      char * pathOriginal = disarmedCmd[1];
      char * pathFinal = disarmedCmd[2];
      if(pathOriginal == NULL || pathFinal == NULL){
    	  log_error(loggerFileSystem, "Faltan parametros para ejecutar el comando mv");
      } else {
    	  moveDirectory(pathOriginal,pathFinal);
      }
    }
    break;
    case 5:{
      if(existePath(disarmedCmd[1])){
    	  printf("Existe path");
      } else {
    	  printf("No existe path");
      }
    }
    break;
    case 6:{
      char* nuevoDirectorio = disarmedCmd[1];
      if(nuevoDirectorio != NULL){
    	  if(!existeDirectory(nuevoDirectorio)){
    		  if(crearDirectorio(nuevoDirectorio) != 1){
    			  log_error(loggerFileSystem, "No se pudo crear el directorio. Por favor vuelva a intentarlo");
    		  }

    	  }else{
    		  log_error(loggerFileSystem, "No se pudo crear el directorio. El directorio ya existe.");
    	  }

      }else{
    	  log_error(loggerFileSystem, "Asegurese de ingresar el nombre del directorio. Por favor vuelva a intentarlo");
      }
    }
    break;
    case 7:{
      char * nombreArchivoViejo = disarmedCmd[1];
      char * nombreArchivoNuevo = disarmedCmd[2];
      char * flag = disarmedCmd[3];
      if(nombreArchivoViejo != NULL && nombreArchivoNuevo != NULL && flag != NULL){
    	  almacenarArchivo(nombreArchivoViejo,nombreArchivoNuevo,flag);
      }else{
    	  log_error(loggerFileSystem, "Faltan parametros para ejecutar el comando cpfrom");
      }
    }
    break;
    case 8:{
    	char * pathArchivoOrigen = disarmedCmd[1];
    	char * directorioFilesystem = disarmedCmd[2];
    	printf("Comando en arreglo! Todavia no se puede ejecutar! (8)\n");
    }
    break;
    case 9:{}

    break;
    case 10:{
    	char* comandoNuevo = string_new();
    	char * nombreArchivoViejo = disarmedCmd[1];
    	if(nombreArchivoViejo == NULL){
    		log_error(loggerFileSystem, "Faltan parametros para ejecutar el comando md5sum");
    	} else {
    		string_append(&comandoNuevo,"md5sum ");
    		string_append(&comandoNuevo,nombreArchivoViejo);
    		system(comandoNuevo);
    		printf("\n");
    	}
    }
    break;
    case 11:{}

    break;
    case 12:{
    	char * nombreArchivoViejo = string_new();
    	string_append(&nombreArchivoViejo, disarmedCmd[1]);
    	if(nombreArchivoViejo == NULL){
    		log_error(loggerFileSystem, "Faltan parametros para ejecutar el comando info.");
    	} else {
        // string_append(&comandoNuevo,"ls -l -h ");
        // string_append(&comandoNuevo,nombreArchivoViejo);
        // system(comandoNuevo);
        // printf("\n");
        //VER SI ES NECESARIO EL SYSTEM
    	}
    	free(nombreArchivoViejo);
    }
    break;
    default:
    	log_warning(loggerFileSystem, "Comando no reconocido, por favor vuelva a intentarlo");
  }
}

void consolaFS(){
  char* command = malloc(100*sizeof(char));
  size_t maxSize = 100*sizeof(char);
  while(1){
    puts("Esperando comando...");
    printf("->");
    getline(&command, &maxSize, stdin);
    uint32_t recvdSize = strlen(command);
    command[recvdSize-1] = '\0';
    if(strcmp(command, "exit")==0){
      free(command);
      pthread_detach(hiloConsolaFS);
      break;
    }
    analizeCommand(command);
  }
}
