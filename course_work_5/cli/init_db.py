from course_work_5.db.loader import load_employers, load_vacancies
from course_work_5.db.migrations import create_database, apply_migrations


def run():
    print('Создание схем...')
    create_database()
    apply_migrations()

    load_employers()
    load_vacancies()


if __name__ == '__main__':
    run()
