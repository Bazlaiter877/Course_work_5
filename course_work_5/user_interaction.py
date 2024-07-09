from prettytable import PrettyTable
from course_work_5.db.managers import PostgresDBManager


def print_employers():
    db_manager = PostgresDBManager()
    try:
        res = db_manager.get_companies_and_vacancies_count()
    finally:
        db_manager.disconnect()

    table = PrettyTable(field_names=['Название компании', 'Количество вакансий'])
    for data in res:
        table.add_row([data[0], data[1]])
    print(table)


def print_average_salary():
    db_manager = PostgresDBManager()
    try:
        salary = db_manager.get_avg_salary()
    finally:
        db_manager.disconnect()

    print(f'Средняя зарплата: {salary} рублей')


def print_all_vacancies():
    db_manager = PostgresDBManager()
    try:
        vacancies = db_manager.get_all_vacancies
    finally:
        db_manager.disconnect()

    table = PrettyTable(field_names=['Название компании', 'Название вакансии', 'Зарплата', 'Ссылка'])
    for vacancy in vacancies:
        table.add_row([vacancy[0], vacancy[1], vacancy[2], vacancy[3]])
    print(table)


def print_vacancies_with_higher_salary():
    db_manager = PostgresDBManager()
    try:
        vacancies = db_manager.get_vacancies_with_higher_salary()
    finally:
        db_manager.disconnect()

    table = PrettyTable(field_names=['Название компании', 'Название вакансии', 'Зарплата', 'Ссылка'])
    for vacancy in vacancies:
        table.add_row([vacancy[0], vacancy[1], vacancy[2], vacancy[3]])
    print(table)


def print_vacancies_with_keyword(keyword):
    db_manager = PostgresDBManager()
    try:
        vacancies = db_manager.get_vacancies_with_keyword(keyword)
    finally:
        db_manager.disconnect()

    table = PrettyTable(field_names=['Название компании', 'Название вакансии', 'Зарплата', 'Ссылка'])
    for vacancy in vacancies:
        table.add_row([vacancy[0], vacancy[1], vacancy[2], vacancy[3]])
    print(table)


def run_interaction():
    while True:
        print(
            'Выберите что сделать:',
            '1 - получить список всех компаний и количество вакансий у каждой компании',
            '2 - получить среднюю зарплату по вакансиям',
            '3 - получить список всех вакансий',
            '4 - получить список вакансий с зарплатой выше средней',
            '5 - получить список вакансий по ключевому слову',
            '0 - выйти',
            sep='\n'
        )
        user_input = input()

        if user_input == '0':
            break
        elif user_input == '1':
            print_employers()
        elif user_input == '2':
            print_average_salary()
        elif user_input == '3':
            print_all_vacancies()
        elif user_input == '4':
            print_vacancies_with_higher_salary()
        elif user_input == '5':
            keyword = input("Введите ключевое слово: ")
            print_vacancies_with_keyword(keyword)
        print()
