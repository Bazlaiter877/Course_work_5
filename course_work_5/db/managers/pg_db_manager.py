import psycopg2
from .base import DBManager


class PostgresDBManager(DBManager):
    def connect(self) -> None:
        if self.connection is None:
            self.connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )

    def disconnect(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def get_companies_and_vacancies_count(self) -> list[tuple[str, int]]:
        sql = """
        SELECT e.name, COUNT(v.id) as vacancies_count
        FROM employers as e
        LEFT JOIN vacancies as v ON e.id = v.employer_id
        GROUP BY e.name
        """
        self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        self.disconnect()
        return result

    def get_avg_salary(self) -> float:
        sql = """SELECT AVG(v.salary_from), AVG(v.salary_to) FROM vacancies as v;"""
        self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            min_salary, max_salary = cursor.fetchone()
        self.disconnect()
        if min_salary and max_salary:
            average_salary = (min_salary + max_salary) / 2
        else:
            average_salary = 0
        return round(average_salary, 2)

    @property
    def get_all_vacancies(self) -> list[tuple[str, str, int, str]]:
        sql = """
        SELECT e.name, v.title, v.salary_from, v.url
        FROM vacancies as v
        JOIN employers as e ON v.employer_id = e.id
        """
        self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
        self.disconnect()
        return result



    def get_vacancies_with_higher_salary(self) -> list[tuple[str, str, int, str]]:
        avg_salary = self.get_avg_salary()
        sql = """
        SELECT e.name, v.title, v.salary_from, v.url
        FROM vacancies as v
        JOIN employers as e ON v.employer_id = e.id
        WHERE v.salary_from > %s
        """
        self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (avg_salary,))
            result = cursor.fetchall()
        self.disconnect()
        return result

    def get_vacancies_with_keyword(self, keyword: str) -> list[tuple[str, str, int, str]]:
        sql = """
        SELECT e.name, v.title, v.salary_from, v.url
        FROM vacancies as v
        JOIN employers as e ON v.employer_id = e.id
        WHERE v.title ILIKE %s
        """
        self.connect()
        with self.connection.cursor() as cursor:
            cursor.execute(sql, (f"%{keyword}%",))
            result = cursor.fetchall()
        self.disconnect()
        return result
