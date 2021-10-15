import psycopg2, csv

def print_separator():
    print("-" * 100)

# open file 
def open_file(file_name):
    try:
        file = open(f"{file_name}.csv", mode="r", encoding='utf-8')
        file_text = list(csv.reader(file))
        titles = file_text[0][0].split(";")
        return file_text, titles
    except FileNotFoundError as ex:
        print_separator()
        print(ex)
        return False, False

# create a new table
def create_table(file_name):
    try:
        create_request = f"CREATE TABLE {file_name}(\nid serial PRIMARY KEY,\n"
        for position in range(len(titles)):
            if position != len(titles)-1:
                create_request = f"{create_request} {titles[position]} varchar(100) NOT NULL,\n"
            else:
                create_request = f"{create_request} {titles[position]} varchar(100) NOT NULL);"
        cursor.execute(create_request)
        print_separator()
        print("[INFO] Table created successfully")
    except Exception as ex:
        print_separator()
        print("[INFO] Error with Database: ", ex)

# fill table
def fill_table(file_name):
    try:
        request_unique_id = f"SELECT {unique_id} FROM {file_name}"
        cursor.execute(request_unique_id)
        response_unique_id = cursor.fetchall()
        unique_id_in_BD = []
        for id in response_unique_id:
            unique_id_in_BD.append(str(id).strip("(),'"))

        count_added = 0
        for position_in_file_text in range(1, len(file_text)):

            values = file_text[position_in_file_text][0].split(";")
            position_unique_id = titles.index(unique_id)

            if values[position_unique_id] not in unique_id_in_BD:
                insert_request = f"INSERT INTO {file_name} ("

                # add titles
                for position in range(len(titles)):
                    if position != len(titles)-1:
                        insert_request = f"{insert_request}{titles[position]}, "
                    else:
                        insert_request = f"{insert_request}{titles[position]}"
                insert_request = f"{insert_request}) VALUES ("
                
                # add values
                for position_value in range(len(values)):
                    if position_value != len(values)-1:
                        insert_request = f"{insert_request}'{values[position_value]}', "
                    else:
                        insert_request = f"{insert_request}'{values[position_value]}'"
                insert_request = f"{insert_request});"
                cursor.execute(insert_request)
                count_added += 1
            else:
                continue
        print_separator()
        print(f"[INFO] Added rows: {count_added}")
    except Exception as ex:
        print_separator()
        print("[INFO] Error with Database: ", ex)

if __name__ == "__main__":
    print_separator()
    host = input("Base address (host): ")
    user = input("Username: ")
    password = input("Password: ")
    database = input("Base name: ")

    while True:
        print_separator()
        action = input("Choose an action.\n1 - create table | 2 - fill table | 3 - exit\n")
        print_separator()
        if action == "1" or action == "2":
            print("CSV file must be in the current directory.")
            file_name = input("Enter file name: ")
            file_text, titles = open_file(file_name)
            if file_text and titles:
                try:
                    connection = psycopg2.connect(
                        host = host,
                        user = user,
                        password = password,
                        database = database
                    )
                    connection.autocommit = True
                    cursor = connection.cursor()

                    if action == "1":
                        create_table(file_name)
                    elif action == "2":
                        unique_id = input("Enter the name of primary key or other unique id: ")
                        fill_table(file_name)

                except Exception as ex:
                    print_separator()
                    print("[INFO] Error with Database: ", ex)
                finally:
                    if connection:
                        cursor.close()
                        connection.close()
                        print("[INFO] Database connection closed")
            else:
                continue
        elif action == "3":
            break
