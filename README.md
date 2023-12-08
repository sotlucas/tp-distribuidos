# TP: Flights Optimizer HA

## Integrantes

- Sotelo Guerreño, Lucas Nahuel - 102730
- Prada, Joaquín - 105978

## Docs

[Consigna](docs/Consigna.pdf)

[Informe](docs/informe.pdf)

## Ejecución

1. Primero es necesario tener los datasets dentro del directorio `data/`:

- Dataset de los precios de vuelos: https://www.kaggle.com/datasets/dilwong/flightprices (con el nombre `archivo.csv`)
    - Dataset de aeropuertos reducido (2 millones de lineas, usado para pruebas): https://www.kaggle.com/datasets/pablodroca/flight-prices-2M
- Dataset de los aeropuertos: https://www.kaggle.com/datasets/pablodroca/airports-opendatahub (con el nombre `airports-codepublic.csv`)

2. Luego, levantar todo con docker compose:

```bash
$ make docker-compose-up
```

Esto ejecuta todos los componentes del sistema y también levanta un cliente que utiliza el archivo `data/archivo.csv`. Los resultados quedarán en el directorio `results/`.

## Check de resultados

Para verificar que los resultados sean correctos se cuenta con un script que compara los resultados obtenidos con los esperados
(usando el archivo de 2 millones de lineas proporcionado a modo de ejemplo). Para ejecutarlo:

```bash
$ make check
```

## Tools

En el directorio `tools/` se encuentran distintos scripts de utilidad.

### Fault Tolerance

En el directorio `tools/fault_tolerance` para probar la tolerancia a fallos se proveen algunos scripts que permiten simular la caida de las distintas entidades del sistema.

