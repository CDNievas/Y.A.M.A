#include <commons/string.h>
#include <commons/log.h>
#include "consolaFS.h"
#include <readline/readline.h>
#include <readline/history.h>


void *consolaFS(){
	char* comando;
	while(1){
		comando = readline("->");
		char* comandoDosCaracteres = string_substring_until(comando, 2);
		char* comandoTresCaracteres = string_substring_until(comando, 3);
		char* comandoCuatroCaracteres = string_substring_until(comando, 4);
		char* comandoCincoCaracteres = string_substring_until(comando, 5);
		char* comandoSeisCaracteres = string_substring_until(comando, 6);
		char* comandoSieteCaracteres = string_substring_until(comando, 7);
		if(comando == NULL){
			break;
		}else if(string_equals_ignore_case(comando, "exit")){
			free(comando);
			exit(0);
		}else if(string_equals_ignore_case(comando, "format")){
			add_history(comando);
		}else if(string_equals_ignore_case(comandoDosCaracteres, "rm")){
			add_history(comandoDosCaracteres);
		}else if(string_equals_ignore_case(comandoDosCaracteres, "mv")){
			add_history(comandoDosCaracteres);
		}else if(string_equals_ignore_case(comandoDosCaracteres, "ls")){
			add_history(comandoDosCaracteres);
		}else if(string_equals_ignore_case(comandoTresCaracteres, "cat")){
			add_history(comandoTresCaracteres);
		}else if(string_equals_ignore_case(comandoTresCaracteres, "md5")){
			add_history(comandoTresCaracteres);
		}else if(string_equals_ignore_case(comandoCuatroCaracteres, "cpto")){
			add_history(comandoCuatroCaracteres);
		}else if(string_equals_ignore_case(comandoCuatroCaracteres, "info")){
			add_history(comandoCuatroCaracteres);
		}else if(string_equals_ignore_case(comandoCincoCaracteres, "mkdir")){
			add_history(comandoCincoCaracteres);
		}else if(string_equals_ignore_case(comandoSeisCaracteres, "rename")){
			add_history(comandoSeisCaracteres);
		}else if(string_equals_ignore_case(comandoSeisCaracteres, "cpfrom")){
			add_history(comandoSeisCaracteres);
		}else if(string_equals_ignore_case(comandoSieteCaracteres, "cpblock")){
			add_history(comandoSieteCaracteres);
		}else{
			//LOGGER DE ERROR PARA QUE PUEDA VOLVER A REINGRESAR EL DATO
		}
	}
}
