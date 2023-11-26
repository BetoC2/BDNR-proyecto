#!/usr/bin/env python3
import os
import pydgraph
import model

from rich.console import Console 
from rich.panel import Panel
from rich.prompt import Prompt

console = Console()

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')



def print_menu():
    mm_options = {
        1: "Create data",
        2: "Show passengers by age staying in a hotel",
        3: "Show adult passengers by airport staying in a hotel",
        4: "Show qualified passengers by airport",
        5: "Show best choices",
        6: "Exit",
    }

    title = "Viva Aerocamion Management System"
    subtitle = "Select an option:"
    menu_items = "\n".join([f"[bold cyan]{key}[/bold cyan]: {mm_options[key]}" for key in mm_options.keys()])
    menu = f"{subtitle}\n{menu_items}"
    console.print(Panel.fit(menu, title=title, border_style="blue"))
    choice = Prompt.ask(f"Enter your choice [blue]\[1-{len(mm_options)}][/blue]")
    return int(choice)


def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)


def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


def close_client_stub(client_stub):
    client_stub.close()


def main():
    # Init Client Stub and Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Create schema
    model.set_schema(client)

    while(True):
        option = print_menu()
        if option == 1:
            model.create_data(client)
        if option == 2:
            age = Prompt.ask("[bold blue]Age[/bold blue]")
            model.get_passengers_by_age(client, age)
            input()
        if option == 3:
            airport = Prompt.ask("[bold blue]Airport[/bold blue]")
            model.get_passengers_by_airport(client, airport)
            input()
        if option == 4:
            airport = Prompt.ask("[bold blue]Airport[/bold blue]")
            model.get_qualified_passengers_by_airport(client, airport)
            input()
        if option == 5:
            model.get_best_choices_airport(client)
            input()
        if option == 6:
            close_client_stub(client_stub)
            exit(0)



if __name__ == '__main__':
    main()
