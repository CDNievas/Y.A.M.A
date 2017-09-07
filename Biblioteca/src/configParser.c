#include <stdio.h>
#include <stdlib.h>
#include <commons/config.h>

// Imprime archivo de configuracion por pantalla
int imprimirConfiguracion(t_config * configuracion, char * parametros[], int tamanioParametros){
	int i = 0;
	int tamanioDic = config_keys_amount(configuracion);

	if(tamanioParametros != tamanioDic){
		return -3;
	}
	while(i < tamanioDic){
		if (!config_has_property(configuracion,parametros[i])){
			return -4;
		}
		printf("%s: %s \n",parametros[i],config_get_string_value(configuracion,parametros[i]));
		i++;
	}
	return 0;
}