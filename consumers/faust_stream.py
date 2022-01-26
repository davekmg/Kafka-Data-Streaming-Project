"""Defines trends calculations for stations"""
import logging
import faust
from dataclasses import asdict, dataclass

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# Note: The parameter topic_partitions has been set inorder to overide the default no of partions
# used for the repartion topic that is created by faust 
# i.e consumers.kafka_streaming_project.faust_stream.station_event-org.chicago.cta.tables.stations-Station.station_name-repartition
app = faust.App(
    "stations-stream-1", 
    broker="kafka://localhost:9092", 
    store="memory://", 
    topic_partitions=5
)

stations_topic = app.topic("org.chicago.cta.tables.stations", value_type=Station)

# This is the output topic
# Note: The transformed_stations_topic should have the same number of partions 
# as stations_topic
transformed_stations_topic = app.topic(
    "org.chicago.cta.stations.table.v1",
    key_type=str,
    value_type=TransformedStation,
    partitions=5,
)

# create Faust table
table = app.Table(
    "stations_table", 
    default=TransformedStation,
    partitions=5,
    changelog_topic=transformed_stations_topic
)

# Note: some stations can have False for all the colors
def get_line(station):
    line = None
    if station.red:
        line = "red"
    elif station.blue:
        line = "blue"
    elif station.green:
        line = "green"
    return line


@app.agent(stations_topic)
async def station_event(station_events):
    # repartition the stream by station_name. Its not working with station_id of type int
    # Note: Keep in mind that the offsets are controlled by the Kafka server 
    # using a consumer group with the same name as the faust app
    async for ev in station_events.group_by(Station.station_name):
        #table[ev.station_name] += ev.order
        #print(f"{ev.station_name}: {table[ev.station_name]}")

        # Note: "line" is the color of the station. 
        table[ev.station_name] = TransformedStation(
                                    station_id=ev.station_id,
                                    station_name=ev.station_name,
                                    order=ev.order,
                                    line=get_line(ev)
                                )
        print(f"table: {asdict(table[ev.station_name])}")


if __name__ == "__main__":
    app.main()
