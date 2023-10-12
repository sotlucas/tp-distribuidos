# TP1: Escalabilidad

## Integrantes

- Sotelo Guerreño, Lucas Nahuel - 102730
- Prada, Joaquín - 105978

## Ejecución 

1. Primero es necesario tener los datasets dentro del directorio `data/`:
- Dataset de los precios de vuelos: https://www.kaggle.com/datasets/dilwong/flightprices (con el nombre `archivo.csv`)
- Dataset de los aeropuertos: https://www.kaggle.com/datasets/pablodroca/airports-opendatahub (con el nombre `airports-codepublic.csv`)
  - *Aclaración*: Si se usa el dataset de vuelos de la cátedra: https://www.kaggle.com/datasets/pablodroca/flight-prices-2m es necesario en el `docker-compose` cambiar la variable de entorno `INPUT_FIELDS` del `filter_general` para que no use la coma del principio, ya que este dataset no la contiene, al contrario del dataset original.

2. Luego, levantar todo con docker compose:

```bash
$ make docker-compose-up
```

Esto ejecuta todos los componentes del sistema y también levanta un cliente que utiliza el archivo `data/archivo.csv`. Los resultados quedarán en el directorio `results/`.
