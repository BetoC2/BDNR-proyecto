#!/usr/bin/env python3
import logging
from rich import print
from rich.table import Table
from rich.console import Console
from collections import defaultdict


log = logging.getLogger()
console = Console()
# airline,from,to,day,month,year,age,gender,reason,stay,transit,connection,wait
months = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "Jun",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}

CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_FLIGHTS_MONTH_TABLE = """
    CREATE TABLE IF NOT EXISTS flights_by_a_m (
        airport TEXT,
        airline TEXT,
        transit TEXT,
        reason TEXT,
        month INT,
        PRIMARY KEY ((airport), month)
    ) WITH CLUSTERING ORDER BY (month ASC);
"""

CREATE_FLIGHTS_MONTH_REASON_TABLE = """
    CREATE TABLE IF NOT EXISTS flights_by_a_m_r (
        airport TEXT,
        airline TEXT,
        transit TEXT,
        reason TEXT,
        month INT,
        PRIMARY KEY ((airport), month, transit, reason)
    ) WITH CLUSTERING ORDER BY (month ASC, transit ASC, reason ASC);
"""

CREATE_INDEX_MONTH = """
    CREATE INDEX IF NOT EXISTS ON flights_by_a_m_r (month)
"""

SELECT_ALL_FLIGHTS = "SELECT * FROM flights_by_a_m"
SELECT_ALL_FLIGHTS_AIRPORT = "SELECT * FROM flights_by_a_m WHERE airport = ?"
SELECT_ALL_FLIGHTS_AIRPORT_MONTH = "SELECT * FROM flights_by_a_m WHERE airport = ? AND month = ?"
SELECT_FLIHGHTS_MONTH_TRANSIT = """
    SELECT airport, month, transit, COUNT(*) AS total
    FROM flights_by_a_m_r
    GROUP BY airport, month, transit
"""

SELECT_FLIGHTS_MONTH_TRANSIT_QUALIFIED = """
    SELECT airport, month, COUNT(*) AS total
    FROM flights_by_a_m_r
    WHERE transit IN ('Airport cab', 'Car rental', 'Mobility as a service', 'Public Transportation', 'Pickup')
    GROUP BY airport, month
    ALLOW FILTERING
"""

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_FLIGHTS_MONTH_TABLE)
    session.execute(CREATE_FLIGHTS_MONTH_REASON_TABLE)
    session.execute(CREATE_INDEX_MONTH)
    
def get_all_flights(session):
    log.info("Getting all flights")
    stmt = session.prepare(SELECT_ALL_FLIGHTS)
    rows = session.execute(stmt)
    table = Table(title="All flights")
    table.add_column("Airport", style="cyan")
    table.add_column("Airline", style="magenta")
    table.add_column("Transit", style="green")
    table.add_column("Reason", style="yellow")
    table.add_column("Month", style="blue")
    for row in rows:
        table.add_row(row.airport, row.airline, row.transit, row.reason, months[row.month])
    print(table)
    
def get_all_flights_airport(session, airport):
    log.info(f"Getting all flights for airport {airport}")
    stmt = session.prepare(SELECT_ALL_FLIGHTS_AIRPORT)
    rows = session.execute(stmt, [airport])
    table = Table(title=f"All flights for airport {airport}")
    table.add_column("Airport", style="cyan")
    table.add_column("Airline", style="magenta")
    table.add_column("Transit", style="green")
    table.add_column("Reason", style="yellow")
    table.add_column("Month", style="blue")
    for row in rows:
        table.add_row(row.airport, row.airline, row.transit, row.reason, months[row.month])
    print(table)
    
def get_all_flights_airport_month(session, airport, month):
    log.info(f"Getting all flights for month {month}")
    stmt = session.prepare(SELECT_ALL_FLIGHTS_AIRPORT_MONTH)
    rows = session.execute(stmt, [airport, month])
    table = Table(title=f"All flights for airport {airport} and month {month}")
    table.add_column("Airport", style="cyan")
    table.add_column("Airline", style="magenta")
    table.add_column("Transit", style="green")
    table.add_column("Reason", style="yellow")
    table.add_column("Month", style="blue")
    for row in rows:
        table.add_row(row.airport, row.airline, row.transit, row.reason, months[row.month])
    print(table)
    
def get_all_flights_month_transit(session):
    log.info(f"Getting all flights for month and transit")
    stmt = session.prepare(SELECT_FLIHGHTS_MONTH_TRANSIT)
    rows = session.execute(stmt)
    table = Table(title=f"All flights for month and transit")
    table.add_column("Airport", style="cyan")
    table.add_column("Month", style="blue")
    table.add_column("Transit", style="green")
    table.add_column("Total", style="yellow")
    for row in rows:
        table.add_row(row.airport, months[row.month], row.transit, str(row.total))
    print(table)

def get_all_flights_month_transit_qualified(session):
    log.info(f"Getting all flights for month and transit")
    stmt = session.prepare(SELECT_FLIGHTS_MONTH_TRANSIT_QUALIFIED)
    rows = session.execute(stmt)
    table = Table(title=f"All flights for month and transit")
    table.add_column("Airport", style="cyan")
    table.add_column("Month", style="blue")
    table.add_column("Total", style="yellow")
    for row in rows:
        table.add_row(row.airport, months[row.month], str(row.total))
    print(table)
    
def get_best_choices(session):
    log.info("Getting the best choices")
    stmt = session.prepare(SELECT_FLIGHTS_MONTH_TRANSIT_QUALIFIED)
    rows = session.execute(stmt)

    airport_months = defaultdict(list)
    max_flights = defaultdict(int)

    # Obtener el máximo número de vuelos para cada aeropuerto y mes
    for row in rows:
        key = (row.airport, row.month)
        airport_months[key].append(row)
        max_flights[row.airport] = max(max_flights[row.airport], row.total)

    # Filtrar los meses con la máxima cantidad de vuelos para cada aeropuerto
    best_choices = []
    for key, flights in airport_months.items():
        if max_flights[key[0]] == flights[0].total:
            best_choices.extend(flights)

    table = Table(title="Best Choices")
    table.add_column("Airport", style="cyan")
    table.add_column("Month", style="blue")
    table.add_column("Total", style="yellow")

    for choice in best_choices:
        table.add_row(choice.airport, months[choice.month], str(choice.total))

    print(table)