# yama-test1
Ejemplo de transformador y reductor para YAMA

## Detalles

Este ejemplo va a tomar los datos del archivo WBAN.csv que contiene la temperatura por hora registrada en una base meteorologica y devolver el maximo de cada base cada dia y la hora de dicho registro.

## Prueba

```
cat WBAN.csv | ./transformador.sh | sort | ./reductor.pl
```

## Resultado

Un registro por dia por base con la mayor temperatura registrada y la hora


### Ejemplo de resultado:

| YYYYMMDD | WBAN  | Temp | HHMM |
|----------|-------|------|------|
| 20130131 | 03011 |  1.0 | 1415 |
| 20130131 | 03012 |  4.0 | 1355 |

### Formato del resultado:

`Fecha (YYYYMMDD);Codigo de WBAN;Temp en C;Hora de medicion (HHMM)`

```
20130131;03011;1.0;1415
20130131;03012;4.0;1355
```



