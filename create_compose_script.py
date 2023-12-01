FILTER_GENERAL_REPLICAS = 5
FILTER_AVG_MAX_REPLICAS = 3
FILTER_MULTIPLE_REPLICAS = 4
FILTER_DISTANCIA_REPLICAS = 4
PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS = 3
FILTER_TRES_ESCALAS_O_MAS_REPLICAS = 1
FILTER_LAT_LONG_REPLICAS = 1
FILTER_DOS_MAS_RAPIDOS_REPLICAS = 1
JOINER_REPLICAS = 5
PROCESSOR_DISTANCIAS_REPLICAS = 1
GROUPER_REPLICAS = 3
PROCESSOR_MAX_AVG_REPLICAS = 1
LOAD_BALANCER_REPLICAS = 3
PROCESSOR_MEDIA_GENERAL_REPLICAS = 1
TAGGER_DOS_MAS_RAPIDOS_REPLICAS = 1
TAGGER_TRES_ESCALAS_O_MAS_REPLICAS = 1
TAGGER_DISTANCIAS_REPLICAS = 1
TAGGER_MAX_AVG_REPLICAS = 1
CLIENT_REPLICAS = 2


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

    def to_dict(self):
        return {
            self.name: {
                "image": self.image,
                "container_name": self.container_name,
                "environment": self.enviroment,
                "healthcheck": self.healthcheck,
                "ports": self.ports,
                "networks": self.networks,
            }
        }


class Entity(Service):
    def __init__(self):
        super().__init__()
        self.environment = {
            "PYTHONUNBUFFERED": "1",
            "LOGGING_LEVEL": "DEBUG",
        }


class Client(Entity):
    def __init__(self):
        super().__init__()
        self.name = "client"
        self.image = "client:latest"
        self.entrypoint = "python3 /main.py archivo.csv airports-codepublic.csv"
        self.depends_on = ["server"]
        self.volumes = [
            "./client/config.ini:/config.ini",
            "./data/archivo.csv:/archivo.csv",
            "./data/airports-codepublic.csv:/airports-codepublic.csv",
            "./results:/results",
        ]


class InsideEntity(Entity):
    def __init__(self):
        super().__init__()
        self.depends_on = {"rabbitmq": {"condition": "service_healthy"}}
        self.entrypoint = "python3 /main.py"
        self.environment["RABBIT_HOST"] = "rabbitmq"


class Server(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "server"
        self.image = "server:latest"
        self.container_name = "server"
        self.volumes = ["./server/config.ini:/config.ini"]
        self.environment["VUELOS_INPUT"] = "vuelos_resultados_listener"
        self.environment["VUELOS_OUTPUT"] = "vuelos"
        self.environment["LAT_LONG_OUTPUT"] = "lat&long"
        self.environment["INPUT_TYPE"] = "EXCHANGE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"


class FilterGeneral(InsideEntity):
    def __init__(self):
        super().__init__()
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


class FilterAvgMax(InsideEntity):
    def __init__(self):
        super().__init__()
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


class FilterMultiple(InsideEntity):
    def __init__(self):
        super().__init__()
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


class FilterDistancia(InsideEntity):
    def __init__(self):
        super().__init__()
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


class ProcessorTresEscalasOMas(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "processor_tres_escalas_o_mas"
        self.image = "tres_escalas_o_mas:latest"
        self.environment["INPUT"] = "vuelos_multiple"
        self.environment["OUTPUT"] = "vuelos_tres_escalas_o_mas"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "EXCHANGE"
        self.environment["REPLICAS_COUNT"] = PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS


class FilterTresEscalasOMas(InsideEntity):
    def __init__(self):
        super().__init__()
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


class FilterDosMasRapidos(InsideEntity):
    def __init__(self):
        super().__init__()
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


class ProcessorDosMasRapidos(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "processor_dos_mas_rapidos"
        self.image = "dos_mas_rapidos:latest"
        self.environment["INPUT"] = "vuelos_dos_mas_rapidos"
        self.environment["OUTPUT"] = "vuelos_dos_mas_rapidos_sink"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = PROCESSOR_TRES_ESCALAS_O_MAS_REPLICAS


class FilterLatLong(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "filter_lat_long"
        self.image = "filter:latest"
        self.environment["DELIMITER"] = ","
        self.environment[
            "INPUT_FIELDS"
        ] = "AirportCode,AirportName,CityName,CountryName,CountryCode,Latitude,Longitude,WorldAreaCode,CityNamegeo_name_id,CountryNamegeo_name_id,coordinates"
        self.environment["OUTPUT_FIELDS"] = "AirportCode,Latitude,Longitude"
        self.environment["INPUT"] = "lat&long"
        self.environment["OUTPUT"] = "lat&long_filtered"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "EXCHANGE"
        self.environment["REPLICAS_COUNT"] = FILTER_LAT_LONG_REPLICAS


class Joiner(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "joiner"
        self.image = "joiner:latest"
        self.environment["VUELOS_INPUT"] = "vuelos_distancia"
        self.environment["LAT_LONG_INPUT"] = "lat&long_filtered"
        self.environment["OUTPUT"] = "vuelos_distancia_joined"
        self.environment["INPUT_TYPE_LAT_LONG"] = "EXCHANGE"
        self.environment["INPUT_TYPE_VUELOS"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = JOINER_REPLICAS


class ProcessorDistancias(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "processor_distancias"
        self.image = "distancias:latest"
        self.environment["INPUT"] = "vuelos_distancia_joined"
        self.environment["OUTPUT"] = "vuelos_distancias_sink"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = PROCESSOR_DISTANCIAS_REPLICAS


class Grouper(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "grouper"
        self.image = "grouper:latest"
        self.environment["VUELOS_INPUT"] = "vuelos_avg_max_balanced"
        self.environment["VUELOS_OUTPUT"] = "vuelos_max_avg_filtered"
        self.environment["MEDIA_GENERAL_INPUT"] = "media_general_sink"
        self.environment["MEDIA_GENERAL_OUTPUT"] = "media_general"
        self.environment["INPUT_TYPE"] = "EXCHANGE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = GROUPER_REPLICAS


class LoadBalancer(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "load_balancer"
        self.image = "load_balancer:latest"
        self.environment["INPUT"] = "vuelos_avg_max"
        self.environment["OUTPUT"] = "vuelos_avg_max_balanced"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "EXCHANGE"
        self.environment["REPLICAS_COUNT"] = LOAD_BALANCER_REPLICAS
        self.environment["REPLGROUPER_REPLICAS_COUNTICAS_COUNT"] = GROUPER_REPLICAS


class ProcessorMediaGeneral(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "processor_media_general"
        self.image = "media_general:latest"
        self.environment["INPUT"] = "media_general"
        self.environment["OUTPUT"] = "media_general_sink"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "EXCHANGE"
        self.environment["REPLICAS_COUNT"] = PROCESSOR_MEDIA_GENERAL_REPLICAS
        self.environment["GROUPER_REPLICAS_COUNT"] = GROUPER_REPLICAS


class ProcessorMaxAvg(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "processor_max_avg"
        self.image = "max_avg:latest"
        self.environment["INPUT"] = "vuelos_max_avg_filtered"
        self.environment["OUTPUT"] = "vuelos_max_avg_sink"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["REPLICAS_COUNT"] = PROCESSOR_MAX_AVG_REPLICAS


class TaggerDosMasRapidos(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "tagger_dos_mas_rapidos"
        self.image = "tagger:latest"
        self.environment["INPUT"] = "vuelos_dos_mas_rapidos_sink"
        self.environment["OUTPUT"] = "vuelos_resultados"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["TAG_NAME"] = "DOS_MAS_RAPIDOS"
        self.environment["REPLICAS_COUNT"] = TAGGER_DOS_MAS_RAPIDOS_REPLICAS


class TaggerTresEscalasOMas(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "tagger_tres_escalas_o_mas"
        self.image = "tagger:latest"
        self.environment["INPUT"] = "vuelos_tres_escalas_o_mas_sink"
        self.environment["OUTPUT"] = "vuelos_resultados"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["TAG_NAME"] = "TRES_ESCALAS"
        self.environment["REPLICAS_COUNT"] = TAGGER_TRES_ESCALAS_O_MAS_REPLICAS


class TaggerDistancias(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "tagger_distancias"
        self.image = "tagger:latest"
        self.environment["INPUT"] = "vuelos_distancias_sink"
        self.environment["OUTPUT"] = "vuelos_resultados"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["TAG_NAME"] = "DISTANCIAS"
        self.environment["REPLICAS_COUNT"] = TAGGER_DISTANCIAS_REPLICAS


class TaggerMaxAvg(InsideEntity):
    def __init__(self):
        super().__init__()
        self.name = "tagger_max_avg"
        self.image = "tagger:latest"
        self.environment["INPUT"] = "vuelos_max_avg_sink"
        self.environment["OUTPUT"] = "vuelos_resultados"
        self.environment["INPUT_TYPE"] = "QUEUE"
        self.environment["OUTPUT_TYPE"] = "QUEUE"
        self.environment["TAG_NAME"] = "MAX_AVG"
        self.environment["REPLICAS_COUNT"] = TAGGER_MAX_AVG_REPLICAS
