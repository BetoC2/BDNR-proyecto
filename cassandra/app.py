#!/usr/bin/env python3
import logging
import os
import random

from cassandra.cluster import Cluster

from rich.console import Console 
from rich.panel import Panel
from rich.prompt import Prompt

console = Console()

import model

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('flights.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'flights')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

def print_menu():
    mm_options = {
        1: "Show all flights",
        2: "Show all flights by airport",
        3: "Show all flights by airport and month",
        4: "Show flights by transit",
        5: "Show qualified flights",
        6: "Show best options for airport",
        7: "Exit",
    }

    title = "Viva Aerocamion Management System"
    subtitle = "Select an option:"
    menu_items = "\n".join([f"[bold cyan]{key}[/bold cyan]: {mm_options[key]}" for key in mm_options.keys()])
    menu = f"{subtitle}\n{menu_items}"
    console.print(Panel.fit(menu, title=title, border_style="blue"))
    choice = Prompt.ask(f"Enter your choice [blue]\[1-{len(mm_options)}][/blue]")
    return int(choice)


def get_instrument_value(instrument):
    instr_mock_sum = sum(bytearray(instrument, encoding='utf-8'))
    return random.uniform(1.0, instr_mock_sum)


def main():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)


    while(True):
        option = print_menu()
        if option == 1:
            model.get_all_flights(session)
            input()
        if option == 2:
            airport = Prompt.ask("Enter airport")
            model.get_all_flights_airport(session, airport)
            input()
        if option == 3:
            airport = Prompt.ask("Enter airport")
            month = Prompt.ask("Enter month [blue]\[1-12][/blue]")
            model.get_all_flights_airport_month(session, airport, int(month))
            input()
        if option == 4:
            model.get_all_flights_month_transit(session)
            input()
        if option == 5:
            model.get_all_flights_month_transit_qualified(session)
            input()
        if option == 6:
            model.get_best_choices(session)
            input()
        if option == 7:
            exit(0)


if __name__ == '__main__':
    main()
