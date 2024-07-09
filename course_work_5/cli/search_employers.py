from prettytable import PrettyTable

from course_work_5.api_clients import HeadHunterAPIClient

hh_client = HeadHunterAPIClient()


def run():
    print('Введите текст для поиска работодателя')
    search = input()

    employers = hh_client.search_employers(search)
    if not employers:
        print('По данному запросу ничего не найдено')
        return

    table = PrettyTable(field_names=['ID', 'Название компании', 'Ссылка', 'Количество вакансий'])
    table.sortby = 'Количество вакансий'
    table.reversesort = True
    for emp in employers:
        table.add_row([emp.id, emp.name, emp.url, emp.open_vacancies])

    print(f'Найдено {len(employers)} компаний')
    print(table)


if __name__ == '__main__':
        run()