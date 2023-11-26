#!/usr/bin/env python3
import json
import csv
import os
import pydgraph

from rich import print
from rich.table import Table

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')
CSV_FILE = './flight_passengers.csv'

def set_schema(client):
    schema = """
        type Passenger {
            age
            gender
            TAKES
        }
        
        age: int @index(int) .
        gender: string .
        TAKES: [uid] @reverse .
        
        type Flight {
            airport
            reason
            stay
            transit: string
        }
            
        airport: string @index(exact) .
        reason: string @index(exact) .
        stay: string @index(exact) .
        transit: string @index(term) .
    """
    return client.alter(pydgraph.Operation(schema=schema))

def create_data(client):
    # Create a new transaction.
    txn = client.txn()
    try:
        passengers = []

        with open(CSV_FILE) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            next(csv_reader)  # Saltar encabezados

            for row in csv_reader:
                airline, fr, to, day, month, year, age, gender, reason, stay, transit, connection, wait = row
                passenger = {
                    "dgraph.type": "Passenger",
                    "age": int(age),
                    "gender": gender,
                    "TAKES": [
                        {
                            "dgraph.type": "Flight",
                            "airport": fr,
                            "reason": reason,
                            "stay": stay,
                            "transit": transit
                        }
                    ]
                }
                passengers.append(passenger)

        response = txn.mutate(set_obj=passengers)

        # Commit transaction.
        commit_response = txn.commit()
        print(f"Commit Response: {commit_response}")

        print(f"UIDs: {response.uids}")
    finally:
        # Clean up.
        txn.discard()



def get_passengers_by_age(client, age):
    query = """query get_hotel_passagers_by_age($a: int) {
        var(func: type(Flight)) @filter(eq(stay, "Hotel")) {
            hoteles as ~TAKES @filter(ge(age, $a)) {
            age
            gender
            TAKES {
                airport
                transit
            }
            }
        }
        passenger(func: uid(hoteles), orderasc: age) {
            uid
            age
            gender
            TAKES {
                airport
                transit
            }
        }
    }"""

    variables = {'$a': age}
    res = client.txn(read_only=True).query(query, variables=variables)
    passengers = json.loads(res.json)
    passengers = passengers['passenger']
    
    table = Table(title="Passenger Information")
    table.add_column("UID", style="cyan")
    table.add_column("Airport", style="blue")
    table.add_column("Age", style="magenta")
    table.add_column("Gender", style="green")
    table.add_column("Stay", style="yellow")
    table.add_column("Transit", style="red")

    for passenger in passengers:
        p = passenger
        uid = p['uid']
        airport = p['TAKES'][0]['airport']
        age_val = p['age']
        gender = p['gender']
        transit = p['TAKES'][0]['transit']
        table.add_row(uid, airport, str(age_val), gender, "Hotel", transit)
        
    print(table)
    print(f"Passengers of age greater or equal to {age}: {len(passengers)}")

def get_passengers_by_airport(client, airport):
    query = """query get_hotel_passagers_by_airport($a: string) {
        var(func: eq(airport, $a)) {
            hasFlightTo as ~TAKES @filter(ge(age, 19)) {
            stay
            age
            gender
            TAKES {
                airport
                transit
            }
            }
        }
        passenger(func: uid(hasFlightTo), orderasc: age) {
            uid
            age
            gender
            TAKES {
                airport
                transit
            }
        }
    }"""

    variables = {'$a': airport}
    res = client.txn(read_only=True).query(query, variables=variables)
    passengers = json.loads(res.json)
    passengers = passengers['passenger']
    
    table = Table(title="Passenger Information")
    table.add_column("Airport", style="blue")
    table.add_column("UID", style="cyan")
    table.add_column("Age", style="magenta")
    table.add_column("Gender", style="green")
    table.add_column("Stay", style="yellow")
    table.add_column("Transit", style="red")

    for passenger in passengers:
        p = passenger
        uid = p['uid']
        age_val = p['age']
        gender = p['gender']
        transit = p['TAKES'][0]['transit']
        table.add_row(airport, uid, str(age_val), gender, "Hotel", transit)
        
        
    print(table)
    print(f"Adult passengers stayin in a hotel at airport {airport}: {len(passengers)}")

def get_qualified_passengers_by_airport(client, airport):
    query = """query get_qualified_passengers_by_airport($a: string) {
        var(func: anyofterms(transit, "rental Mobility Public Transportation Pickup")) @filter(eq(airport, $a)) {
            hasFlightTo as ~TAKES @filter(ge(age, 18)) {
            stay
            age
            gender
            TAKES {
                airport
                transit
            }
            }
        }
        passengerCount(func: uid(hasFlightTo)) {
            totalCount: count(uid)
        }
    }"""
    
    variables = {'$a': airport}
    res = client.txn(read_only=True).query(query, variables=variables)
    total = json.loads(res.json)
    total = total["passengerCount"][0]["totalCount"]
    
    print(f"Total adult qualified passengers on airport {airport}: {total}")

def get_best_choices_airport(client):
    airports = ["PDX", "GDL", "SJC", "LAX", "JFK"]
    results = {}

    query = """
    query get_qualified_passengers_by_airport($a: string) {
        var(func: anyofterms(transit, "rental Mobility Public Transportation Pickup")) @filter(eq(airport, $a)) {
            hasFlightTo as ~TAKES @filter(ge(age, 18)) {
                stay
                age
                gender
                TAKES {
                    airport
                    transit
                }
            }
        }
        passengerCount(func: uid(hasFlightTo)) {
            totalCount: count(uid)
        }
    }"""

    for airport in airports:
        variables = {'$a': airport}
        res = client.txn(read_only=True).query(query, variables=variables)
        total = json.loads(res.json)
        total = total["passengerCount"][0]["totalCount"]
        results[airport] = total

    sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)

    table = Table(title="Qualified Passengers by Airport")
    table.add_column("Airport", style="cyan")
    table.add_column("Total", style="magenta")

    for airport, total in sorted_results:
        table.add_row(airport, str(total))

    print(table)

def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))


"""
{
  var(func: anyofterms(transit, "rental Mobility Public Transportation Pickup")) @filter(eq(airport, "GDL")) {
    hasFlightTo as ~TAKES @filter(ge(age, 19)) {
      stay
      age
      gender
      TAKES {
        airport
        transit
      }
    }
  }
  passenger(func: uid(hasFlightTo), orderasc: age) {
    uid
    age
    gender
    TAKES {
      airport
      transit
    }
  }
}

"""