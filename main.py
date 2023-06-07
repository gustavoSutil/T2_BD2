import json
import psycopg2
from config import BD_CONFIGURATION as db

def connect_to_database() -> psycopg2.connect :
    # Conectar ao banco de dados
    conn = psycopg2.connect(
        host=db.config["host"],
        database=db.config["database"],
        user=db.config["user"],
        password=db.config["password"]
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

def load_log(log_file) -> (list,list):
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


def check_data(started,commited,values,conn):
    non_commited = list(set(started)-set(commited))

    need_undo = []
    need_redo = []
    for i in range(len(values)):
        for id_transaction in non_commited:
            if id_transaction==values[i][0]:
                #acha as transaçoes que nao realizaram o commit
                need_undo.append(values[i])
        for id_transaction in commited:
            if id_transaction==values[i][0]:
                #acha as transaçoes do log que realizaram o commit
                need_redo.append(values[i])

    r = redo(need_redo, conn)
    u = undo(need_undo,conn)


def undo(operations,conn):
    result = []
    #vai ver se o dado no banco esta indevidamente no banco
    #exemplo [0:'id_transaction', 1:'id_tupla', 2:'colunm', 3'old', 4:'new']
    cursor = conn.cursor()
    for t in operations:
        try:
            cursor.execute(f"SELECT {t[2]} FROM initial WHERE id = '{t[1]}'")
            r = cursor.fetchall()
            if r[0][0] == t[4]:
                cursor.execute(f"UPDATE initial SET {t[2]} = {t[3]} WHERE id = '{t[1]}';")
                print(f"Transação {t[0]} realizou UNDO")
        except Exception as e:
            print("Ocorreu um erro: ", e)
    cursor.close()
    conn.commit()
    return result

def redo(operations, conn) -> list:
    #vai ver se o dado esta diferente se sim atualiza no banco
    #exemplo [0:'id_transaction', 1:'id_tupla', 2:'colunm', 3'old', 4:'new']
    result = []
    cursor = conn.cursor()
    for t in operations:
        try:
            cursor.execute(f"SELECT {t[2]} FROM initial WHERE id = '{t[1]}'")
            r = cursor.fetchall()
            if r[0][0] != t[4]:
                cursor.execute(f"UPDATE initial SET {t[2]} = {t[4]} WHERE id = '{t[1]}';")
                print(f"Transação {t[0]} realizou REDO")
        except Exception as e:
            print("Ocorreu um erro: ", e)
    cursor.close()
    conn.commit()
    return result

def main():
    with open('metadado.json', 'r') as file:
        metadado = json.load(file) # Carregar metadados


    values, process = load_log('log.txt')

    started, commited = verifyProcess(process) # retorna os processos que foram comitados


    conn = connect_to_database() # Conectar ao banco de dados

    # o main das operaçoes REDU/UNDU retorna o que foi feito
    actions = check_data(started,commited,values,conn)


    conn.close() #Fechar a conexão com o banco de dados



if __name__ == '__main__':
    main()
