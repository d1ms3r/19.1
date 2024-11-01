import csv

with open('customers_data.csv', newline='') as file:
    customers_data = [row for row in csv.reader(file) if 'customer_id' not in row]

with open('employees_data.csv', newline='') as file:
    employees_data = [row for row in csv.reader(file) if 'first_name' not in row]

with open('orders_data.csv', newline='') as file:
    orders_data = [row for row in csv.reader(file) if 'order_id' not in row]

# Импортируйте библиотеку psycopg2
import psycopg2

# Создайте подключение к базе данных
conn = psycopg2.connect(
    dbname='analysis',
    user='simple',
    password='qweasd963',
    port='5432',
    host='sql_db'
)
# Открытие курсора
cur = conn.cursor()

# Не меняйте и не удаляйте эти строки - они нужны для проверки
cur.execute("create schema if not exists itresume4887;")
cur.execute("DROP TABLE IF EXISTS itresume4887.orders")
cur.execute("DROP TABLE IF EXISTS itresume4887.customers")
cur.execute("DROP TABLE IF EXISTS itresume4887.employees")

# Ниже напишите код запросов для создания таблиц
cur.execute(
    "CREATE TABLE itresume4887.customers(customer_id CHAR(5) PRIMARY KEY, company_name varchar(100) NOT NULL, "
    "contact_name varchar(100) NOT NULL);")
cur.execute(
    "CREATE TABLE itresume4887.employees(employee_id serial PRIMARY KEY, first_name varchar(25) NOT NULL, "
    "last_name varchar(25) NOT NULL, title varchar(100) NOT NULL, birth_date DATE NOT NULL, notes text);")
cur.execute(
    "CREATE TABLE itresume4887.orders(order_id int PRIMARY KEY, customer_id CHAR(5) REFERENCES customers("
    "customer_id), employee_id int REFERENCES employees(employee_id), order_date DATE NOT NULL, ship_city varchar(25) "
    "NOT NULL);")

# Зафиксируйте изменения в базе данных
conn.commit()

# Теперь приступаем к операциям вставок данных
# Запустите цикл по списку customers_data и выполните запрос формата
# INSERT INTO itresume3270.table (column1, column2, ...) VALUES (%s, %s, ...) returning ", data)
# В конце каждого INSERT-запроса обязательно должен быть оператор returning

for nn in customers_data:
    cur.execute(
        "INSERT INTO itresume4887.customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s) returning *",
        nn)

# Не меняйте и не удаляйте эти строки - они нужны для проверки
conn.commit()
res_customers = cur.fetchall()

# Запустите цикл по списку employees_data и выполните запрос формата
# INSERT INTO itresume4887.table (column1, column2, ...) VALUES (%s, %s, ...) returning *", data)
# В конце каждого INSERT-запроса обязательно должен быть оператор returning *
for nm in employees_data:
    # print(nm) # Для отладки: выводим данные из текущей строки
    cur.execute(
        "INSERT INTO itresume4887.employees(first_name, last_name, title, birth_date, notes) VALUES (%s, %s, %s, %s, "
        "%s) returning *",
        nm)

# Не меняйте и не удаляйте эти строки - они нужны для проверки
conn.commit()
res_employees = cur.fetchall()

# Запустите цикл по списку orders_data и выполните запрос формата
# INSERT INTO itresume4887.table (column1, column2, ...) VALUES (%s, %s, ...) returning *", data)
# В конце каждого INSERT-запроса обязательно должен быть оператор returning *
for ns in orders_data:
    cur.execute(
        "INSERT INTO itresume4887.orders (order_id, customer_id, employee_id, order_date, ship_city) VALUES (%s, %s, "
        "%s, %s, %s) returning *",
        ns)

# Не меняйте и не удаляйте эти строки - они нужны для проверки
conn.commit()
res_orders = cur.fetchall()

# Закрытие курсора
cur.close()

# Закрытие соединения
conn.close()
