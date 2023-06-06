import json
import psycopg2

def connect_to_database():
    # Conectar ao banco de dados
    conn = psycopg2.connect(
        host="localhost",
        database="db_log",
        user="postgres",
        password="pgadmin"
    )
    return conn

def load_database(conn, table_data):
    #  Carregar o banco de dados com a tabela inicial

    cursor = conn.cursor()
    cursor.execute("DELETE FROM initial")
    conn.commit()

    for id_t in range(len(table_data['id'])):
        cursor.execute(f"INSERT INTO initial (id,A,B) VALUES ({table_data['id'][id_t]},{table_data['A'][id_t]},{table_data['B'][id_t]})")
    conn.commit()

def load_log(log_file):
    # ler o arquivo de log e retornar os valores e os processos feitos
    with open(log_file, 'r') as file:
        log_data = file.read().splitlines()
        transactions = []
        process = []
        for line in log_data:
            line = line.strip()
            if line.startswith("<"):
                transaction = line.strip("<>").split(",")
                if len(transaction) == 5:
                    transactions.append(transaction)
                else:
                    process.append(transaction)
        return transactions, process

def verifyProcess(process) -> (list, list):
    # ver se realizou commit pois caso nao tenha
    # sera necessario fazer redu (isso pois deveria ter acusado o roollback)

    started = []
    commited = []

    for content in process:
        for transaction in content:
            operation, id_transaction = transaction.split(" ")
            if operation == "start":
                started.append(id_transaction)
            elif operation == "commit":
                commited.append(id_transaction)

    return started, commited





def main():
    with open('metadado.json', 'r') as file:
        metadado = json.load(file) # Carregar metadados



    values, process = load_log('log.txt')
    started, commited = verifyProcess(process)


    conn = connect_to_database() # Conectar ao banco de dados




    # Fechar a conexão com o banco de dados
    conn.close()

if __name__ == '__main__':
    main()
