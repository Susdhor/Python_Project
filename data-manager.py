from cmath import nan
import sqlite3
import csv


class DataManager:
    # Protected variable
    _database_name = "vaccination_info.db"

    # Empty Constructor
    def __init__(self):
        pass

    # Creating Database and necessary Tables with relational constraints
    def create_database(self):
        db_connection = sqlite3.connect(self._database_name)
        db_cursor = db_connection.cursor()
        db_cursor.execute("""CREATE TABLE IF NOT EXISTS vaccin_covid_csv (
                    country text DEFAULT NULL, 
                    iso_code text DEFAULT NULL, 
                    date text DEFAULT NULL, 
                    total_vaccinations real DEFAULT 0, 
                    people_vaccinated real DEFAULT 0, 
                    people_fully_vaccinated real DEFAULT 0, 
                    daily_vaccinations_raw real DEFAULT 0, 
                    daily_vaccinations real DEFAULT 0, 
                    total_vaccinations_per_hundred real DEFAULT 0, 
                    people_vaccinated_per_hundred real DEFAULT 0, 
                    people_fully_vaccinated_per_hundred real DEFAULT 0, 
                    daily_vaccinations_per_million real DEFAULT 0, 
                    vaccines text DEFAULT NULL, 
                    source_name text DEFAULT NULL, 
                    source_website text DEFAULT NULL)""")

        db_cursor.execute("""CREATE TABLE IF NOT EXISTS countries (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            iso_code TEXT NOT NULL
        )""")

        db_cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS countries_iso_code_IDX ON countries (iso_code)""")
        db_cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS countries_name_IDX ON countries (name)""")

        db_cursor.execute("""CREATE TABLE IF NOT EXISTS vaccines (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            "source" TEXT NOT NULL,
            source_website TEXT NOT NULL
        )""")

        db_cursor.execute("""CREATE TABLE IF NOT EXISTS vaccine_info (
            country_id INTEGER NOT NULL,
            vaccine_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_vaccinations REAL DEFAULT 0 NOT NULL,
            people_vaccinated REAL DEFAULT 0 NOT NULL,
            people_fully_vaccinated REAL DEFAULT 0 NOT NULL,
            daily_vaccinations_raw TEXT DEFAULT 0 NOT NULL,
            daily_vaccinations REAL DEFAULT 0 NOT NULL,
            total_vaccinations_per_hundred REAL DEFAULT 0 NOT NULL,
            people_vaccinated_per_hundred REAL DEFAULT 0 NOT NULL,
            people_fully_vaccinated_per_hundred REAL DEFAULT 0 NOT NULL,
            daily_vaccinations_per_million REAL DEFAULT 0 NOT NULL,
            CONSTRAINT vaccine_info_FK FOREIGN KEY (country_id) REFERENCES countries(id) ON DELETE RESTRICT,
            CONSTRAINT vaccine_info_FK_1 FOREIGN KEY (vaccine_id) REFERENCES vaccines(id) ON DELETE RESTRICT
        )""")

    def seed_database(self, file_name):
        db_connection = sqlite3.connect(self._database_name)
        db_cursor = db_connection.cursor()
        reader = csv.reader(open(file_name, 'r'), delimiter=',')
        next(reader)
        for row in reader:
            columns = [row[0],
                       row[1],
                       row[2],
                       row[3],
                       row[4],
                       row[5],
                       row[6],
                       row[7],
                       row[8],
                       row[9],
                       row[10],
                       row[11],
                       row[12],
                       row[13],
                       row[14]
                       ]

            db_cursor.execute("""INSERT INTO 
                         vaccin_covid_csv(country, iso_code, date, total_vaccinations, people_vaccinated,
                         people_fully_vaccinated, daily_vaccinations_raw, daily_vaccinations, total_vaccinations_per_hundred, 
                         people_vaccinated_per_hundred, people_fully_vaccinated_per_hundred, 
                         daily_vaccinations_per_million, vaccines, source_name, source_website
                         ) 
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", columns)
        db_connection.commit()
        self.__normalize_data()


    def __normalize_data(self):
        db_connection = sqlite3.connect(self._database_name)
        db_cursor = db_connection.cursor()
        db_cursor.execute(
            "insert into countries(name, iso_code) select country, iso_code from vaccin_covid_csv group by country")
        db_connection.commit()
        db_cursor.execute(
            "insert into vaccines(name, source, source_website) select vaccines, source_name , source_website  from vaccin_covid_csv group by vaccines")
        db_connection.commit()
        # Collecting all the country code and ids in a dictionary so that we don't have to srearch the database each time we update a row
        countries = self.__get_country_ids()
        # Collecting all the vaccine name and ids in a dictionary so that we don't have to srearch the database each time we update a row
        vaccines = self.__get_vaccine_ids()
        # Collecting all the original records loaded from csv file for updating in the relational tables
        db_cursor.execute("select * from vaccin_covid_csv")
        records = db_cursor.fetchall()
        normalized_db_connection = sqlite3.connect(self._database_name)
        normalized_db_cursor = normalized_db_connection.cursor()
        for row in records:
            country_id = countries[row[1]]
            vaccine_id = vaccines[row[12]]
            columns = [country_id,
                       vaccine_id,
                       row[2],
                       row[3],
                       row[4],
                       row[5],
                       row[6],
                       row[7],
                       row[8],
                       row[9],
                       row[10],
                       row[11]
                       ]
            self.__insert_normalized_row(normalized_db_cursor, columns)
        normalized_db_connection.commit()
        normalized_db_cursor.close()
        normalized_db_connection.close()

    def __insert_normalized_row(self, db_cursor, columns):
        db_cursor.execute("""insert into vaccine_info(country_id, vaccine_id, date, total_vaccinations, people_vaccinated,
                         people_fully_vaccinated, daily_vaccinations_raw, daily_vaccinations, total_vaccinations_per_hundred, 
                         people_vaccinated_per_hundred, people_fully_vaccinated_per_hundred, 
                         daily_vaccinations_per_million ) values(?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?)""", columns)

    def __get_country_ids(self):
        db_connection = sqlite3.connect(self._database_name)
        db_cursor = db_connection.cursor()
        db_cursor.execute("select iso_code, id from countries")
        countries = {}
        records = db_cursor.fetchall()
        for row in records:
            countries[row[0]] = row[1]
        return countries

    def __get_vaccine_ids(self):
        db_connection = sqlite3.connect(self._database_name)
        db_cursor = db_connection.cursor()
        db_cursor.execute("select name, id from vaccines")
        vaccines = {}
        records = db_cursor.fetchall()
        for row in records:
            vaccines[row[0]] = row[1]
        return vaccines

    def clean_database(self):
        self._db_cursor.execute("DROP TABLE vaccin_covid_csv")
        self._db_connection.commit()



if __name__ == "__main__":
    data_manager = DataManager()
    data_manager.create_database()
    data_manager.seed_database('vaccin_covid.csv')
    # data_manager.clean_database()
