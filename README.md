# Public Transit Status with Apache Kafka

This project is a streaming event pipeline around Apache Kafka and its ecosystem. Using public data from the [Chicago Transit Authority](https://www.transitchicago.com/data/) an event pipeline has been constructed around Kafka that allows us to simulate and display the status of train lines in real time.

When the project is run, it provides a web interface that allows you to monitor trains as they move from station to station.

![Final User Interface](images/ui.png)


## Prerequisites

The following are required to run this project:

* Docker
* Python 3.7+
* Access to a computer with a minimum of 16gb+ RAM and a 4-core CPU to execute the simulation

## Description

The Chicago Transit Authority (CTA) has asked us to develop a dashboard displaying system status for its commuters. We have decided to use Kafka and ecosystem tools like REST Proxy and Kafka Connect to accomplish this task.

Our architecture will look like so:

![Project Architecture](images/diagram.png)


## Directory Layout
The project consists of two main directories, `producers` and `consumers`.



```
├── consumers
│   ├── consumer.py
│   ├── faust_stream.py
│   ├── ksql.py
│   ├── models
│   │   ├── lines.py
│   │   ├── line.py
│   │   ├── station.py
│   │   └── weather.py
│   ├── requirements.txt
│   ├── server.py
│   ├── topic_check.py
│   └── templates
│       └── status.html
└── producers
    ├── connector.py
    ├── models
    │   ├── line.py
    │   ├── producer.py
    │   ├── schemas
    │   │   ├── arrival_key.json
    │   │   ├── arrival_value.json
    │   │   ├── turnstile_key.json
    │   │   ├── turnstile_value.json
    │   │   ├── weather_key.json
    │   │   └── weather_value.json
    │   ├── station.py
    │   ├── train.py
    │   ├── turnstile.py
    │   ├── turnstile_hardware.py
    │   └── weather.py
    ├── requirements.txt
    └── simulation.py
```

## Running and Testing

To run the simulation, you must first start up the Kafka ecosystem on the machine utilizing Docker Compose.

```%> docker-compose up -d```

Docker compose will take a 3-5 minutes to start, depending on your hardware. Please be patient and wait for the docker-compose logs to slow down or stop before beginning the simulation.

### Install Kafka Connect JDBC Plugin
The latest Kafka Connect docker image, as per this writting, does not come pre-installed with the JDBC plugin. Run the commands below to intall the plugin.

```%> docker exec -it connect /bin/bash```

```%> confluent-hub install confluentinc/kafka-connect-jdbc:latest```

Important! When istalling, you will be prompted with the below. Make sure to select option 2.

```
The component can be installed in any of the following Confluent Platform installations: 
1. / (installed rpm/deb package) 
2. / (where this tool is installed) 
```

For the rest of the options choose y

Important! Restart the Kafka Connect docker container after installation. Run the command below on the host machine. Give it afew minutes (3 min) to complete the restart.

```%> docker restart connect ```

Once docker-compose is ready, the following services will be available:

| Service | Host URL | Docker URL | Username | Password |
| --- | --- | --- | --- | --- |
| Public Transit Status | [http://localhost:8888](http://localhost:8888) | n/a | ||
| Kafka Control Center | [http://localhost:9021](http://localhost:9021) | http://control-center:9021 |
| Kafka | PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094 | PLAINTEXT://kafka0:9092,PLAINTEXT://kafka1:9093,PLAINTEXT://kafka2:9094 |
| REST Proxy | [http://localhost:8082](http://localhost:8082/) | http://rest-proxy:8082/ |
| Schema Registry | [http://localhost:8081](http://localhost:8081/ ) | http://schema-registry:8081/ |
| Kafka Connect | [http://localhost:8083](http://localhost:8083) | http://kafka-connect:8083 |
| KSQL | [http://localhost:8088](http://localhost:8088) | http://ksql:8088 |
| PostgreSQL | `jdbc:postgresql://localhost:5432/cta` | `jdbc:postgresql://postgres:5432/cta` | `cta_admin` | `chicago` |

Note that to access these services from your own machine, you will always use the `Host URL` column.

When configuring services that run within Docker Compose, like **Kafka Connect you must use the Docker URL**. When you configure the JDBC Source Kafka Connector, for example, you will want to use the value from the `Docker URL` column.

### Prepare the Postgres Database
1. Copy files from host to the docker container \
```%> docker cp './producers/data' postgres:/tmp```

2. Access bash \
```%> docker exec -it  postgres /bin/bash```

3. Login to postrgess database cta with the user cta_admin \
```%> psql -d cta -U cta_admin```

4. Run the sql script to create the stations table and copy data into it \
```%> \i /tmp/data/load_stations.sql```

### Running the Simulation

There are two pieces to the simulation, the `producer` and `consumer`. 

To run the project, it is critical that you open a terminal window for each piece and run them at the same time. **If you do not run both the producer and consumer at the same time you will not be able to successfully run the project**.


#### To run the `producer`:
1. `cd producers`
2. `python3 -m venv .venv`
3. `source .venv/bin/activate`
4. `pip install -r requirements.txt`
5. `python3 simulation.py`

Once the simulation is running, you may hit `Ctrl+C` at any time to exit.

#### To run the Faust Stream Processing Application:
1. `cd consumers`
2. `python3 -m venv .venv`
3. `source .venv/bin/activate`
4. `pip install -r requirements.txt`
5. `faust -A faust_stream worker -l info`

#### To run the KSQL Creation Script:
1. `cd consumers`
2. `source .venv/bin/activate`
3. `python3 ksql.py`

#### To run the `consumer`:
1. `cd consumers`
2. `source .venv/bin/activate`
3. `python server.py`

Once the server is running, you may hit `Ctrl+C` at any time to exit.
