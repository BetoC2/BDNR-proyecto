import pymongo
from rich.console import Console
from rich.table import Table

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['project']
collection = db['flights']

console = Console()

def airport_connections():
    result = collection.find({"connection": "True"})
    return result

def count_connections_by_airport():
    result = collection.aggregate([
        {"$match": {"connection": "True"}},
        {"$group": {"_id": "$to", "count": {"$sum": 1}}}
    ])
    return result

def average_wait_by_airport():
    result = collection.aggregate([
        {"$match": {"connection": "True"}},
        {"$group": {"_id": "$to", "average_wait": {"$avg": {"$toDouble": "$wait"}}}}
    ])
    return result

def average_age_by_airport():
    result = collection.aggregate([
        {"$match": {"connection": "True"}},
        {"$group": {"_id": "$to", "average_age": {"$avg": {"$toDouble": "$age"}}}}
    ])
    return result

def print_menu_table():
    console.print("\n\t\t\t[bold magenta]VIVA AEROCAMION[/bold magenta]")
    menu_table = Table(show_header=True, header_style="bold blue")
    menu_table.add_column("Option", style="bold yellow", justify="center")
    menu_table.add_column("Description", style="bold green")

    menu_table.add_row("1", "Show only airports with connection flights")
    menu_table.add_row("2", "Show number of connection flights by airport")
    menu_table.add_row("3", "Show average waiting time by airport")
    menu_table.add_row("4", "Show passengers average age by airport")  # Nueva opción
    menu_table.add_row("5", "Exit")

    console.print(menu_table)

def main():
    while True:
        print_menu_table()
        choice = input("Enter your choice [1-5]: ")

        if choice == "1":
            connection_documents = airport_connections()
            unique_destinations = set(doc['to'] for doc in connection_documents)
            console.print("\n[bold blue]Airports with connection flights:[/bold blue]")
            for destination in unique_destinations:
                console.print(f"[green]{destination}[/green]")
            input()

        elif choice == "2":
            connection_counts = count_connections_by_airport()
            console.print("\n[bold blue]Connection flights by airport:[/bold blue]")
            for entry in connection_counts:
                console.print(f"[cyan]{entry['_id']}:[/cyan] [yellow]{entry['count']}[/yellow] connection flights")
            input()

        elif choice == "3":
            wait_averages = average_wait_by_airport()
            console.print("\n[bold blue]Average waiting time by airport:[/bold blue]")
            for entry in wait_averages:
                average_wait_rounded = round(entry['average_wait'], 2)
                console.print(f"[cyan]{entry['_id']}:[/cyan] [yellow]{average_wait_rounded}[/yellow] minutes")
            input()

        elif choice == "4":
            age_averages = average_age_by_airport()
            console.print("\n[bold blue]Passengers average age by airport:[/bold blue]")
            for entry in age_averages:
                average_age_rounded = round(entry['average_age'])
                console.print(f"[cyan]{entry['_id']}:[/cyan] [yellow]{average_age_rounded}[/yellow] years")
            input()

        elif choice == "5":
            console.print("[bold magenta]Thanks for using Viva Aerocamion[/bold magenta]")
            break
        else:
            console.print("[bold red]Invalid option. Try again[/bold red]")

if __name__ == "__main__":
    main()

