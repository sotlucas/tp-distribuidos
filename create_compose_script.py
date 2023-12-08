LOGGING_LEVEL = "DEBUG"

CLIENT_REPLICAS = 2
FILTER_GENERAL_REPLICAS = 6
FILTER_AVG_MAX_REPLICAS = 6
FILTER_MULTIPLE_REPLICAS = 6
FILTER_DISTANCIA_REPLICAS = 6
FILTER_TRES_ESCALAS_O_MAS_REPLICAS = 6
FILTER_LAT_LONG_REPLICAS = 3
FILTER_DOS_MAS_RAPIDOS_REPLICAS = 6
PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS = 6
PROCESSOR_DISTANCIAS_REPLICAS = 6
PROCESSOR_MAX_AVG_REPLICAS = 3
LOAD_BALANCER_REPLICAS = 12
JOINER_REPLICAS = 6
GROUPER_REPLICAS = 12

# Don't change
SERVER_REPLICAS = 1
HEALTH_CHECKER_REPLICAS = 3
PROCESSOR_MEDIA_GENERAL_REPLICAS = 1
PROCESSOR_DOS_MAS_RAPIDOS_REPLICAS = 1
TAGGER_DOS_MAS_RAPIDOS_REPLICAS = 1
TAGGER_TRES_ESCALAS_O_MAS_REPLICAS = 1
TAGGER_DISTANCIAS_REPLICAS = 1
TAGGER_MAX_AVG_REPLICAS = 1

# TODO: Los taggers ahora no se pueden replicar por si solos, ya que tienen un duplicate catcher dentro.
#       Con poner un load balancer antes de cada tagger, se soluciona el problema en parte.
#       Ya que el load balancer tiene un pequeño bug que es que asume que le manda por cada batch de mensajes que recibe
#       un mensaje individual a todas las réplicas, esto no sería un problema si no fuera porque las queries de dos mas rápidos
#       y max avg tienen pocos mensajes que se mandan, haciendo que no se manden mensajes a todas las réplicas.
#       Por lo tanto, solucionando esto se podría replicar los taggers.
#       Igualmente se podrían replicar con load balancer en las otras queries, la de Distancias y la de Tres escalas o más.


class Service:
    def __init__(self):
        self.networks = ["testing_net"]


class RabbitMQ(Service):
    def __init__(self):
        super().__init__()
        self.name = "rabbitmq"
        self.image = "rabbitmq:management"
        self.container_name = "rabbitmq"
        self.enviroment = {
            "RABBITMQ_DEFAULT_USER": "guest",
            "RABBITMQ_DEFAULT_PASS": "guest",
        }
        self.healthcheck = {
            "test": "rabbitmq-diagnostics check_port_connectivity",
            "interval": "5s",
            "timeout": "3s",
            "retries": "10",
            "start_period": "50s",
        }
        self.ports = ["5672:5672", "15672:15672"]

    def __str__(self):
        return f"""
  {self.name}:
    image: {self.image}
    container_name: {self.container_name}
    environment:
      - RABBITMQ_DEFAULT_USER={self.enviroment["RABBITMQ_DEFAULT_USER"]}
      - RABBITMQ_DEFAULT_PASS={self.enviroment["RABBITMQ_DEFAULT_PASS"]}
    healthcheck:
      test: {self.healthcheck["test"]}
      interval: {self.healthcheck["interval"]}
      timeout: {self.healthcheck["timeout"]}
      retries: {self.healthcheck["retries"]}
      start_period: {self.healthcheck["start_period"]}
    networks:
      - {self.networks[0]}
    ports:
      - "{self.ports[0]}"
      - "{self.ports[1]}"
"""


class Entity(Service):
    def __init__(self, replica_id=1):
        super().__init__()
        self.replica_id = replica_id
        self.environment = {
            "PYTHONUNBUFFERED": "1",
            "LOGGING_LEVEL": LOGGING_LEVEL,
        }


class Client(Entity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "client"
        self.image = "client:latest"
        self.entrypoint = "python3 /main.py archivo.csv airports-codepublic.csv"
        self.depends_on = ["server_1"]
        self.volumes = [
            "./client/config.ini:/config.ini",
            "./data/archivo.csv:/archivo.csv",
            "./data/airports-codepublic.csv:/airports-codepublic.csv",
            "./results:/results",
        ]

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - REPLICA_ID={self.replica_id}
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
    depends_on:
      - {self.depends_on[0]}
    networks:
      - {self.networks[0]}
    volumes:
      - {self.volumes[0]}
      - {self.volumes[1]}
      - {self.volumes[2]}
      - {self.volumes[3]}
"""


class InsideEntity(Entity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.depends_on = {"rabbitmq": {"condition": "service_healthy"}}
        self.entrypoint = "python3 /main.py"
        self.environment["RABBIT_HOST"] = "rabbitmq"


class Server(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "server"
        self.image = "server:latest"
        self.volumes = ["./server/config.ini:/config.ini"]
        self.environment["VUELOS_INPUT"] = "vuelos_resultados_listener"
        self.environment["VUELOS_OUTPUT"] = "vuelos"
        self.environment["LAT_LONG_OUTPUT"] = "lat&long"
        self.environment["INPUT_TYPE"] = "EXCHANGE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - VUELOS_INPUT={self.environment["VUELOS_INPUT"]}
      - VUELOS_OUTPUT={self.environment["VUELOS_OUTPUT"]}
      - LAT_LONG_OUTPUT={self.environment["LAT_LONG_OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
    volumes:
      - {self.volumes[0]}
"""


class FilterGeneral(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "filter_general"
        self.image = "filter:latest"
        self.environment["DELIMITER"] = ","
        self.environment[
            "INPUT_FIELDS"
        ] = "legId,searchDate,flightDate,startingAirport,destinationAirport,fareBasisCode,travelDuration,elapsedDays,isBasicEconomy,isRefundable,isNonStop,baseFare,totalFare,seatsRemaining,totalTravelDistance,segmentsDepartureTimeEpochSeconds,segmentsDepartureTimeRaw,segmentsArrivalTimeEpochSeconds,segmentsArrivalTimeRaw,segmentsArrivalAirportCode,segmentsDepartureAirportCode,segmentsAirlineName,segmentsAirlineCode,segmentsEquipmentDescription,segmentsDurationInSeconds,segmentsDistance,segmentsCabinCode"
        self.environment[
            "OUTPUT_FIELDS"
        ] = "legId,startingAirport,destinationAirport,totalFare,totalTravelDistance,travelDuration,segmentsArrivalAirportCode"
        self.environment["INPUT"] = "vuelos"
        self.environment["OUTPUT"] = "vuelos_filtered"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "EXCHANGE"
        self.environment["REPLICAS_COUNT"] = FILTER_GENERAL_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - DELIMITER={self.environment["DELIMITER"]}
      - INPUT_FIELDS={self.environment["INPUT_FIELDS"]}
      - OUTPUT_FIELDS={self.environment["OUTPUT_FIELDS"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class FilterAvgMax(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "filter_avg_max"
        self.image = "filter:latest"
        self.environment["DELIMITER"] = ","
        self.environment[
            "INPUT_FIELDS"
        ] = "legId,startingAirport,destinationAirport,totalFare,totalTravelDistance,travelDuration,segmentsArrivalAirportCode"
        self.environment[
            "OUTPUT_FIELDS"
        ] = "startingAirport,destinationAirport,totalFare"
        self.environment["INPUT"] = "vuelos_filtered"
        self.environment["OUTPUT"] = "vuelos_avg_max"
        self.environment["INPUT_TYPE"] = "EXCHANGE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = FILTER_AVG_MAX_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - DELIMITER={self.environment["DELIMITER"]}
      - INPUT_FIELDS={self.environment["INPUT_FIELDS"]}
      - OUTPUT_FIELDS={self.environment["OUTPUT_FIELDS"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class FilterMultiple(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "filter_multiple"
        self.image = "filter:latest"
        self.environment["DELIMITER"] = ","
        self.environment[
            "INPUT_FIELDS"
        ] = "legId,startingAirport,destinationAirport,totalFare,totalTravelDistance,travelDuration,segmentsArrivalAirportCode"
        self.environment[
            "OUTPUT_FIELDS"
        ] = "legId,startingAirport,destinationAirport,totalFare,travelDuration,segmentsArrivalAirportCode"
        self.environment["INPUT"] = "vuelos_filtered"
        self.environment["OUTPUT"] = "vuelos_multiple"
        self.environment["INPUT_TYPE"] = "EXCHANGE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = FILTER_MULTIPLE_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - DELIMITER={self.environment["DELIMITER"]}
      - INPUT_FIELDS={self.environment["INPUT_FIELDS"]}
      - OUTPUT_FIELDS={self.environment["OUTPUT_FIELDS"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class FilterDistancia(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "filter_distancia"
        self.image = "filter:latest"
        self.environment["DELIMITER"] = ","
        self.environment[
            "INPUT_FIELDS"
        ] = "legId,startingAirport,destinationAirport,totalFare,totalTravelDistance,travelDuration,segmentsArrivalAirportCode"
        self.environment[
            "OUTPUT_FIELDS"
        ] = "legId,startingAirport,destinationAirport,totalTravelDistance"
        self.environment["INPUT"] = "vuelos_filtered"
        self.environment["OUTPUT"] = "vuelos_distancia"
        self.environment["INPUT_TYPE"] = "EXCHANGE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = FILTER_DISTANCIA_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - DELIMITER={self.environment["DELIMITER"]}
      - INPUT_FIELDS={self.environment["INPUT_FIELDS"]}
      - OUTPUT_FIELDS={self.environment["OUTPUT_FIELDS"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class ProcessorTresEscalasOMas(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "processor_tres_escalas_o_mas"
        self.image = "tres_escalas_o_mas:latest"
        self.environment["INPUT"] = "vuelos_multiple"
        self.environment["OUTPUT"] = "vuelos_tres_escalas_o_mas"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "EXCHANGE"
        self.environment["REPLICAS_COUNT"] = PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class FilterTresEscalasOMas(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "filter_tres_escalas_o_mas"
        self.image = "filter:latest"
        self.environment["DELIMITER"] = ","
        self.environment[
            "INPUT_FIELDS"
        ] = "legId,startingAirport,destinationAirport,totalFare,travelDuration,segmentsArrivalAirportCode"
        self.environment[
            "OUTPUT_FIELDS"
        ] = "legId,startingAirport,destinationAirport,totalFare,segmentsArrivalAirportCode"
        self.environment["INPUT"] = "vuelos_tres_escalas_o_mas"
        self.environment["OUTPUT"] = "vuelos_tres_escalas_o_mas_sink"
        self.environment["INPUT_TYPE"] = "EXCHANGE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = FILTER_TRES_ESCALAS_O_MAS_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - DELIMITER={self.environment["DELIMITER"]}
      - INPUT_FIELDS={self.environment["INPUT_FIELDS"]}
      - OUTPUT_FIELDS={self.environment["OUTPUT_FIELDS"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class FilterDosMasRapidos(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "filter_dos_mas_rapidos"
        self.image = "filter:latest"
        self.environment["DELIMITER"] = ","
        self.environment[
            "INPUT_FIELDS"
        ] = "legId,startingAirport,destinationAirport,totalFare,travelDuration,segmentsArrivalAirportCode"
        self.environment[
            "OUTPUT_FIELDS"
        ] = "legId,startingAirport,destinationAirport,travelDuration,segmentsArrivalAirportCode"
        self.environment["INPUT"] = "vuelos_tres_escalas_o_mas"
        self.environment["OUTPUT"] = "vuelos_dos_mas_rapidos"
        self.environment["INPUT_TYPE"] = "EXCHANGE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = FILTER_DOS_MAS_RAPIDOS_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - DELIMITER={self.environment["DELIMITER"]}
      - INPUT_FIELDS={self.environment["INPUT_FIELDS"]}
      - OUTPUT_FIELDS={self.environment["OUTPUT_FIELDS"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class ProcessorDosMasRapidos(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "processor_dos_mas_rapidos"
        self.image = "dos_mas_rapidos:latest"
        self.environment["INPUT"] = "vuelos_dos_mas_rapidos"
        self.environment["OUTPUT"] = "vuelos_dos_mas_rapidos_sink"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class FilterLatLong(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "filter_lat_long"
        self.image = "filter:latest"
        self.environment["DELIMITER"] = ";"
        self.environment[
            "INPUT_FIELDS"
        ] = "AirportCode,AirportName,CityName,CountryName,CountryCode,Latitude,Longitude,WorldAreaCode,CityNamegeo_name_id,CountryNamegeo_name_id,coordinates"
        self.environment["OUTPUT_FIELDS"] = "AirportCode,Latitude,Longitude"
        self.environment["INPUT"] = "lat&long"
        self.environment["OUTPUT"] = "lat&long_filtered"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "EXCHANGE"
        self.environment["REPLICAS_COUNT"] = FILTER_LAT_LONG_REPLICAS

    def __str__(self):
        first = f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - DELIMITER={self.environment["DELIMITER"]}
      - INPUT_FIELDS={self.environment["INPUT_FIELDS"]}
      - OUTPUT_FIELDS={self.environment["OUTPUT_FIELDS"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}"""
        depends_on = """
    depends_on:
      rabbitmq:
        condition: service_healthy"""
        for i in range(1, JOINER_REPLICAS + 1):
            joiner = f"""
      joiner_{i}:
        condition: service_started"""
            depends_on += joiner
        networks = f"""
    networks:
        - {self.networks[0]}
"""
        return first + depends_on + networks


class Joiner(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "joiner"
        self.image = "joiner:latest"
        self.environment["VUELOS_INPUT"] = "vuelos_distancia"
        self.environment["LAT_LONG_INPUT"] = "lat&long_filtered"
        self.environment["OUTPUT"] = "vuelos_distancia_joined"
        self.environment["INPUT_TYPE_LAT_LONG"] = "EXCHANGE"
        self.environment["INPUT_TYPE_VUELOS"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = JOINER_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - VUELOS_INPUT={self.environment["VUELOS_INPUT"]}
      - LAT_LONG_INPUT={self.environment["LAT_LONG_INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE_LAT_LONG={self.environment["INPUT_TYPE_LAT_LONG"]}
      - INPUT_TYPE_VUELOS={self.environment["INPUT_TYPE_VUELOS"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class ProcessorDistancias(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "processor_distancias"
        self.image = "distancias:latest"
        self.environment["INPUT"] = "vuelos_distancia_joined"
        self.environment["OUTPUT"] = "vuelos_distancias_sink"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = PROCESSOR_DISTANCIAS_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class Grouper(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "grouper"
        self.image = "grouper:latest"
        self.environment["VUELOS_INPUT"] = "vuelos_avg_max_balanced"
        self.environment["VUELOS_OUTPUT"] = "vuelos_max_avg_filtered"
        self.environment["MEDIA_GENERAL_INPUT"] = "media_general_sink"
        self.environment["MEDIA_GENERAL_OUTPUT"] = "media_general"
        self.environment["INPUT_TYPE"] = "EXCHANGE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = GROUPER_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - VUELOS_INPUT={self.environment["VUELOS_INPUT"]}
      - VUELOS_OUTPUT={self.environment["VUELOS_OUTPUT"]}
      - MEDIA_GENERAL_INPUT={self.environment["MEDIA_GENERAL_INPUT"]}
      - MEDIA_GENERAL_OUTPUT={self.environment["MEDIA_GENERAL_OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class LoadBalancer(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "load_balancer"
        self.image = "load_balancer:latest"
        self.environment["INPUT"] = "vuelos_avg_max"
        self.environment["OUTPUT"] = "vuelos_avg_max_balanced"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "EXCHANGE"
        self.environment["REPLICAS_COUNT"] = LOAD_BALANCER_REPLICAS
        self.environment["GROUPER_REPLICAS_COUNT"] = GROUPER_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - GROUPER_REPLICAS_COUNT={self.environment["GROUPER_REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class ProcessorMediaGeneral(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "processor_media_general"
        self.image = "media_general:latest"
        self.environment["INPUT"] = "media_general"
        self.environment["OUTPUT"] = "media_general_sink"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "EXCHANGE"
        self.environment["GROUPER_REPLICAS_COUNT"] = GROUPER_REPLICAS
        self.environment["REPLICAS_COUNT"] = PROCESSOR_MEDIA_GENERAL_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - GROUPER_REPLICAS_COUNT={self.environment["GROUPER_REPLICAS_COUNT"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class ProcessorMaxAvg(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "processor_max_avg"
        self.image = "max_avg:latest"
        self.environment["INPUT"] = "vuelos_max_avg_filtered"
        self.environment["OUTPUT"] = "vuelos_max_avg_sink"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = PROCESSOR_MAX_AVG_REPLICAS
        self.environment["GROUPER_REPLICAS_COUNT"] = GROUPER_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - GROUPER_REPLICAS_COUNT={self.environment["GROUPER_REPLICAS_COUNT"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class TaggerDosMasRapidos(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "tagger_dos_mas_rapidos"
        self.image = "tagger:latest"
        self.environment["INPUT"] = "vuelos_dos_mas_rapidos_sink"
        self.environment["OUTPUT"] = "vuelos_resultados"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["TAG_NAME"] = "DOS_MAS_RAPIDOS"
        self.environment["TAG_ID"] = 1
        self.environment["REPLICAS_COUNT"] = TAGGER_DOS_MAS_RAPIDOS_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - TAG_NAME={self.environment["TAG_NAME"]}
      - TAG_ID={self.environment["TAG_ID"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class TaggerTresEscalasOMas(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "tagger_tres_escalas_o_mas"
        self.image = "tagger:latest"
        self.environment["INPUT"] = "vuelos_tres_escalas_o_mas_sink"
        self.environment["OUTPUT"] = "vuelos_resultados"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["TAG_NAME"] = "TRES_ESCALAS"
        self.environment["TAG_ID"] = 2
        self.environment["REPLICAS_COUNT"] = TAGGER_TRES_ESCALAS_O_MAS_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - TAG_NAME={self.environment["TAG_NAME"]}
      - TAG_ID={self.environment["TAG_ID"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class TaggerDistancias(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "tagger_distancias"
        self.image = "tagger:latest"
        self.environment["INPUT"] = "vuelos_distancias_sink"
        self.environment["OUTPUT"] = "vuelos_resultados"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["TAG_NAME"] = "DISTANCIAS"
        self.environment["TAG_ID"] = 3
        self.environment["REPLICAS_COUNT"] = TAGGER_DISTANCIAS_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - TAG_NAME={self.environment["TAG_NAME"]}
      - TAG_ID={self.environment["TAG_ID"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class TaggerMaxAvg(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "tagger_max_avg"
        self.image = "tagger:latest"
        self.environment["INPUT"] = "vuelos_max_avg_sink"
        self.environment["OUTPUT"] = "vuelos_resultados"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["TAG_NAME"] = "MAX_AVG"
        self.environment["TAG_ID"] = 4
        self.environment["REPLICAS_COUNT"] = TAGGER_MAX_AVG_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - INPUT={self.environment["INPUT"]}
      - OUTPUT={self.environment["OUTPUT"]}
      - RABBIT_HOST={self.environment["RABBIT_HOST"]}
      - INPUT_TYPE={self.environment["INPUT_TYPE"]}
      - OUTPUT_TYPE={self.environment["OUTPUT_TYPE"]}
      - REPLICAS_COUNT={self.environment["REPLICAS_COUNT"]}
      - TAG_NAME={self.environment["TAG_NAME"]}
      - TAG_ID={self.environment["TAG_ID"]}
      - REPLICA_ID={self.replica_id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class HealthChecker1(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "health_checker"
        self.image = "health_checker:latest"
        self.entrypoint = "python3 /main.py"
        self.environment["FILTER_GENERAL_REPLICAS"] = FILTER_GENERAL_REPLICAS
        self.environment["FILTER_MULTIPLE_REPLICAS"] = FILTER_MULTIPLE_REPLICAS
        self.environment["FILTER_AVG_MAX_REPLICAS"] = FILTER_AVG_MAX_REPLICAS
        self.environment["FILTER_DISTANCIA_REPLICAS"] = FILTER_DISTANCIA_REPLICAS
        self.environment[
            "FILTER_TRES_ESCALAS_O_MAS_REPLICAS"
        ] = FILTER_TRES_ESCALAS_O_MAS_REPLICAS
        self.environment[
            "FILTER_DOS_MAS_RAPIDOS_REPLICAS"
        ] = FILTER_DOS_MAS_RAPIDOS_REPLICAS
        self.environment["FILTER_LAT_LONG_REPLICAS"] = FILTER_LAT_LONG_REPLICAS
        self.environment["PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS"] = 0
        self.environment["PROCESSOR_DOS_MAS_RAPIDOS_REPLICAS"] = 0
        self.environment["PROCESSOR_DISTANCIAS_REPLICAS"] = 0
        self.environment["PROCESSOR_MAX_AVG_REPLICAS"] = 0
        self.environment["PROCESSOR_MEDIA_GENERAL_REPLICAS"] = 0
        self.environment["TAGGER_DOS_MAS_RAPIDOS_REPLICAS"] = 0
        self.environment["TAGGER_TRES_ESCALAS_O_MAS_REPLICAS"] = 0
        self.environment["TAGGER_DISTANCIAS_REPLICAS"] = 0
        self.environment["TAGGER_MAX_AVG_REPLICAS"] = 0
        self.environment["LOAD_BALANCER_REPLICAS"] = 0
        self.environment["GROUPER_REPLICAS"] = 0
        self.environment["JOINER_REPLICAS"] = 0
        self.environment["SERVER_REPLICAS"] = 0
        self.environment["HEALTH_CHECKER_REPLICAS"] = HEALTH_CHECKER_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - REPLICA_ID={self.replica_id}
      - FILTER_GENERAL_REPLICAS={self.environment["FILTER_GENERAL_REPLICAS"]}
      - FILTER_MULTIPLE_REPLICAS={self.environment["FILTER_MULTIPLE_REPLICAS"]}
      - FILTER_AVG_MAX_REPLICAS={self.environment["FILTER_AVG_MAX_REPLICAS"]}
      - FILTER_DISTANCIA_REPLICAS={self.environment["FILTER_DISTANCIA_REPLICAS"]}
      - FILTER_TRES_ESCALAS_O_MAS_REPLICAS={self.environment["FILTER_TRES_ESCALAS_O_MAS_REPLICAS"]}
      - FILTER_DOS_MAS_RAPIDOS_REPLICAS={self.environment["FILTER_DOS_MAS_RAPIDOS_REPLICAS"]}
      - FILTER_LAT_LONG_REPLICAS={self.environment["FILTER_LAT_LONG_REPLICAS"]}
      - PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS={self.environment["PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS"]}
      - PROCESSOR_DOS_MAS_RAPIDOS_REPLICAS={self.environment["PROCESSOR_DOS_MAS_RAPIDOS_REPLICAS"]}
      - PROCESSOR_DISTANCIAS_REPLICAS={self.environment["PROCESSOR_DISTANCIAS_REPLICAS"]}
      - PROCESSOR_MAX_AVG_REPLICAS={self.environment["PROCESSOR_MAX_AVG_REPLICAS"]}
      - PROCESSOR_MEDIA_GENERAL_REPLICAS={self.environment["PROCESSOR_MEDIA_GENERAL_REPLICAS"]}
      - TAGGER_DOS_MAS_RAPIDOS_REPLICAS={self.environment["TAGGER_DOS_MAS_RAPIDOS_REPLICAS"]}
      - TAGGER_TRES_ESCALAS_O_MAS_REPLICAS={self.environment["TAGGER_TRES_ESCALAS_O_MAS_REPLICAS"]}
      - TAGGER_DISTANCIAS_REPLICAS={self.environment["TAGGER_DISTANCIAS_REPLICAS"]}
      - TAGGER_MAX_AVG_REPLICAS={self.environment["TAGGER_MAX_AVG_REPLICAS"]}
      - LOAD_BALANCER_REPLICAS={self.environment["LOAD_BALANCER_REPLICAS"]}
      - GROUPER_REPLICAS={self.environment["GROUPER_REPLICAS"]}
      - JOINER_REPLICAS={self.environment["JOINER_REPLICAS"]}
      - SERVER_REPLICAS={self.environment["SERVER_REPLICAS"]}
      - HEALTH_CHECKER_REPLICAS={self.environment["HEALTH_CHECKER_REPLICAS"]}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class HealthChecker2(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "health_checker"
        self.image = "health_checker:latest"
        self.entrypoint = "python3 /main.py"
        self.environment["FILTER_GENERAL_REPLICAS"] = 0
        self.environment["FILTER_MULTIPLE_REPLICAS"] = 0
        self.environment["FILTER_AVG_MAX_REPLICAS"] = 0
        self.environment["FILTER_DISTANCIA_REPLICAS"] = 0
        self.environment["FILTER_TRES_ESCALAS_O_MAS_REPLICAS"] = 0
        self.environment["FILTER_DOS_MAS_RAPIDOS_REPLICAS"] = 0
        self.environment["FILTER_LAT_LONG_REPLICAS"] = 0
        self.environment[
            "PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS"
        ] = PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS
        self.environment[
            "PROCESSOR_DOS_MAS_RAPIDOS_REPLICAS"
        ] = PROCESSOR_DOS_MAS_RAPIDOS_REPLICAS
        self.environment[
            "PROCESSOR_DISTANCIAS_REPLICAS"
        ] = PROCESSOR_DISTANCIAS_REPLICAS
        self.environment["PROCESSOR_MAX_AVG_REPLICAS"] = PROCESSOR_MAX_AVG_REPLICAS
        self.environment[
            "PROCESSOR_MEDIA_GENERAL_REPLICAS"
        ] = PROCESSOR_MEDIA_GENERAL_REPLICAS
        self.environment["TAGGER_DOS_MAS_RAPIDOS_REPLICAS"] = 0
        self.environment["TAGGER_TRES_ESCALAS_O_MAS_REPLICAS"] = 0
        self.environment["TAGGER_DISTANCIAS_REPLICAS"] = 0
        self.environment["TAGGER_MAX_AVG_REPLICAS"] = 0
        self.environment["LOAD_BALANCER_REPLICAS"] = 0
        self.environment["GROUPER_REPLICAS"] = 0
        self.environment["JOINER_REPLICAS"] = 0
        self.environment["SERVER_REPLICAS"] = 0
        self.environment["HEALTH_CHECKER_REPLICAS"] = HEALTH_CHECKER_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - REPLICA_ID={self.replica_id}
      - FILTER_GENERAL_REPLICAS={self.environment["FILTER_GENERAL_REPLICAS"]}
      - FILTER_MULTIPLE_REPLICAS={self.environment["FILTER_MULTIPLE_REPLICAS"]}
      - FILTER_AVG_MAX_REPLICAS={self.environment["FILTER_AVG_MAX_REPLICAS"]}
      - FILTER_DISTANCIA_REPLICAS={self.environment["FILTER_DISTANCIA_REPLICAS"]}
      - FILTER_TRES_ESCALAS_O_MAS_REPLICAS={self.environment["FILTER_TRES_ESCALAS_O_MAS_REPLICAS"]}
      - FILTER_DOS_MAS_RAPIDOS_REPLICAS={self.environment["FILTER_DOS_MAS_RAPIDOS_REPLICAS"]}
      - FILTER_LAT_LONG_REPLICAS={self.environment["FILTER_LAT_LONG_REPLICAS"]}
      - PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS={self.environment["PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS"]}
      - PROCESSOR_DOS_MAS_RAPIDOS_REPLICAS={self.environment["PROCESSOR_DOS_MAS_RAPIDOS_REPLICAS"]}
      - PROCESSOR_DISTANCIAS_REPLICAS={self.environment["PROCESSOR_DISTANCIAS_REPLICAS"]}
      - PROCESSOR_MAX_AVG_REPLICAS={self.environment["PROCESSOR_MAX_AVG_REPLICAS"]}
      - PROCESSOR_MEDIA_GENERAL_REPLICAS={self.environment["PROCESSOR_MEDIA_GENERAL_REPLICAS"]}
      - TAGGER_DOS_MAS_RAPIDOS_REPLICAS={self.environment["TAGGER_DOS_MAS_RAPIDOS_REPLICAS"]}
      - TAGGER_TRES_ESCALAS_O_MAS_REPLICAS={self.environment["TAGGER_TRES_ESCALAS_O_MAS_REPLICAS"]}
      - TAGGER_DISTANCIAS_REPLICAS={self.environment["TAGGER_DISTANCIAS_REPLICAS"]}
      - TAGGER_MAX_AVG_REPLICAS={self.environment["TAGGER_MAX_AVG_REPLICAS"]}
      - LOAD_BALANCER_REPLICAS={self.environment["LOAD_BALANCER_REPLICAS"]}
      - GROUPER_REPLICAS={self.environment["GROUPER_REPLICAS"]}
      - JOINER_REPLICAS={self.environment["JOINER_REPLICAS"]}
      - SERVER_REPLICAS={self.environment["SERVER_REPLICAS"]}
      - HEALTH_CHECKER_REPLICAS={self.environment["HEALTH_CHECKER_REPLICAS"]}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


class HealthChecker3(InsideEntity):
    def __init__(self, replica_id=1):
        super().__init__(replica_id)
        self.name = "health_checker"
        self.image = "health_checker:latest"
        self.entrypoint = "python3 /main.py"
        self.environment["FILTER_GENERAL_REPLICAS"] = 0
        self.environment["FILTER_MULTIPLE_REPLICAS"] = 0
        self.environment["FILTER_AVG_MAX_REPLICAS"] = 0
        self.environment["FILTER_DISTANCIA_REPLICAS"] = 0
        self.environment["FILTER_TRES_ESCALAS_O_MAS_REPLICAS"] = 0
        self.environment["FILTER_DOS_MAS_RAPIDOS_REPLICAS"] = 0
        self.environment["FILTER_LAT_LONG_REPLICAS"] = 0
        self.environment["PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS"] = 0
        self.environment["PROCESSOR_DOS_MAS_RAPIDOS_REPLICAS"] = 0
        self.environment["PROCESSOR_DISTANCIAS_REPLICAS"] = 0
        self.environment["PROCESSOR_MAX_AVG_REPLICAS"] = 0
        self.environment["PROCESSOR_MEDIA_GENERAL_REPLICAS"] = 0
        self.environment[
            "TAGGER_DOS_MAS_RAPIDOS_REPLICAS"
        ] = TAGGER_DOS_MAS_RAPIDOS_REPLICAS
        self.environment[
            "TAGGER_TRES_ESCALAS_O_MAS_REPLICAS"
        ] = TAGGER_TRES_ESCALAS_O_MAS_REPLICAS
        self.environment["TAGGER_DISTANCIAS_REPLICAS"] = TAGGER_DISTANCIAS_REPLICAS
        self.environment["TAGGER_MAX_AVG_REPLICAS"] = TAGGER_MAX_AVG_REPLICAS
        self.environment["LOAD_BALANCER_REPLICAS"] = LOAD_BALANCER_REPLICAS
        self.environment["GROUPER_REPLICAS"] = GROUPER_REPLICAS
        self.environment["JOINER_REPLICAS"] = JOINER_REPLICAS
        self.environment["SERVER_REPLICAS"] = SERVER_REPLICAS
        self.environment["HEALTH_CHECKER_REPLICAS"] = HEALTH_CHECKER_REPLICAS

    def __str__(self):
        return f"""
  {self.name}_{self.replica_id}:
    image: {self.image}
    entrypoint: {self.entrypoint}
    environment:
      - PYTHONUNBUFFERED={self.environment["PYTHONUNBUFFERED"]}
      - LOGGING_LEVEL={self.environment["LOGGING_LEVEL"]}
      - REPLICA_ID={self.replica_id}
      - FILTER_GENERAL_REPLICAS={self.environment["FILTER_GENERAL_REPLICAS"]}
      - FILTER_MULTIPLE_REPLICAS={self.environment["FILTER_MULTIPLE_REPLICAS"]}
      - FILTER_AVG_MAX_REPLICAS={self.environment["FILTER_AVG_MAX_REPLICAS"]}
      - FILTER_DISTANCIA_REPLICAS={self.environment["FILTER_DISTANCIA_REPLICAS"]}
      - FILTER_TRES_ESCALAS_O_MAS_REPLICAS={self.environment["FILTER_TRES_ESCALAS_O_MAS_REPLICAS"]}
      - FILTER_DOS_MAS_RAPIDOS_REPLICAS={self.environment["FILTER_DOS_MAS_RAPIDOS_REPLICAS"]}
      - FILTER_LAT_LONG_REPLICAS={self.environment["FILTER_LAT_LONG_REPLICAS"]}
      - PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS={self.environment["PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS"]}
      - PROCESSOR_DOS_MAS_RAPIDOS_REPLICAS={self.environment["PROCESSOR_DOS_MAS_RAPIDOS_REPLICAS"]}
      - PROCESSOR_DISTANCIAS_REPLICAS={self.environment["PROCESSOR_DISTANCIAS_REPLICAS"]}
      - PROCESSOR_MAX_AVG_REPLICAS={self.environment["PROCESSOR_MAX_AVG_REPLICAS"]}
      - PROCESSOR_MEDIA_GENERAL_REPLICAS={self.environment["PROCESSOR_MEDIA_GENERAL_REPLICAS"]}
      - TAGGER_DOS_MAS_RAPIDOS_REPLICAS={self.environment["TAGGER_DOS_MAS_RAPIDOS_REPLICAS"]}
      - TAGGER_TRES_ESCALAS_O_MAS_REPLICAS={self.environment["TAGGER_TRES_ESCALAS_O_MAS_REPLICAS"]}
      - TAGGER_DISTANCIAS_REPLICAS={self.environment["TAGGER_DISTANCIAS_REPLICAS"]}
      - TAGGER_MAX_AVG_REPLICAS={self.environment["TAGGER_MAX_AVG_REPLICAS"]}
      - LOAD_BALANCER_REPLICAS={self.environment["LOAD_BALANCER_REPLICAS"]}
      - GROUPER_REPLICAS={self.environment["GROUPER_REPLICAS"]}
      - JOINER_REPLICAS={self.environment["JOINER_REPLICAS"]}
      - SERVER_REPLICAS={self.environment["SERVER_REPLICAS"]}
      - HEALTH_CHECKER_REPLICAS={self.environment["HEALTH_CHECKER_REPLICAS"]}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {self.networks[0]}
"""


def main():
    services = [RabbitMQ()]

    for i in range(1, CLIENT_REPLICAS + 1):
        services.append(Client(i))

    services.append(Server())

    for i in range(1, FILTER_GENERAL_REPLICAS + 1):
        services.append(FilterGeneral(i))

    for i in range(1, FILTER_AVG_MAX_REPLICAS + 1):
        services.append(FilterAvgMax(i))

    for i in range(1, FILTER_MULTIPLE_REPLICAS + 1):
        services.append(FilterMultiple(i))

    for i in range(1, FILTER_DISTANCIA_REPLICAS + 1):
        services.append(FilterDistancia(i))

    for i in range(1, PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS + 1):
        services.append(ProcessorTresEscalasOMas(i))

    for i in range(1, FILTER_TRES_ESCALAS_O_MAS_REPLICAS + 1):
        services.append(FilterTresEscalasOMas(i))

    for i in range(1, FILTER_DOS_MAS_RAPIDOS_REPLICAS + 1):
        services.append(FilterDosMasRapidos(i))

    for i in range(1, PROCESSOR_DOS_MAS_RAPIDOS_REPLICAS + 1):
        services.append(ProcessorDosMasRapidos(i))

    for i in range(1, FILTER_LAT_LONG_REPLICAS + 1):
        services.append(FilterLatLong(i))

    for i in range(1, JOINER_REPLICAS + 1):
        services.append(Joiner(i))

    for i in range(1, PROCESSOR_DISTANCIAS_REPLICAS + 1):
        services.append(ProcessorDistancias(i))

    for i in range(1, GROUPER_REPLICAS + 1):
        services.append(Grouper(i))

    for i in range(1, LOAD_BALANCER_REPLICAS + 1):
        services.append(LoadBalancer(i))

    for i in range(1, PROCESSOR_MEDIA_GENERAL_REPLICAS + 1):
        services.append(ProcessorMediaGeneral(i))

    for i in range(1, PROCESSOR_MAX_AVG_REPLICAS + 1):
        services.append(ProcessorMaxAvg(i))

    for i in range(1, TAGGER_DOS_MAS_RAPIDOS_REPLICAS + 1):
        services.append(TaggerDosMasRapidos(i))

    for i in range(1, TAGGER_TRES_ESCALAS_O_MAS_REPLICAS + 1):
        services.append(TaggerTresEscalasOMas(i))

    for i in range(1, TAGGER_DISTANCIAS_REPLICAS + 1):
        services.append(TaggerDistancias(i))

    for i in range(1, TAGGER_MAX_AVG_REPLICAS + 1):
        services.append(TaggerMaxAvg(i))

    services.append(HealthChecker1(1))
    services.append(HealthChecker2(2))
    services.append(HealthChecker3(3))

    with open("docker-compose.yml", "w") as f:
        f.write('version: "3.4"\n')
        f.write("name: tp1\n")
        f.write("\n")
        f.write("services:")
        for service in services:
            f.write(str(service))
        f.write("\n")
        f.write("networks:\n")
        f.write("  testing_net:\n")
        f.write("    ipam:\n")
        f.write("      driver: default\n")
        f.write("      config:\n")
        f.write("        - subnet: 172.25.125.0/24")


if __name__ == "__main__":
    main()
