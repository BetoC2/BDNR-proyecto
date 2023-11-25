import csv

CQL_FILE = './data.cql'
CSV_FILE = './flight_passengers.csv'

def cql_stmt_generator():
    with open(CQL_FILE, 'w') as cqlFile:
        with open(CSV_FILE, 'r') as csvFile:
            data = csv.reader(csvFile)
            headers = next(data)
            for row in data:
                airline, fr, to, day, month, year, age, gender, reason, stay, transit, connection, wait = row
                cqlFile.write(f"INSERT INTO flights_by_a_m (airport, airline, transit, reason, month) VALUES ('{to}', '{airline}', '{transit}', '{reason}', {month});\n")
                cqlFile.write(f"INSERT INTO flights_by_a_m_r (airport, airline, transit, reason, month) VALUES ('{to}', '{airline}', '{transit}', '{reason}', {month});\n")

def main():
    cql_stmt_generator()

if __name__ == '__main__':
    main()
