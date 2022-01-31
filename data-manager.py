import sqlite3
import csv

class DataManager:
    # Protected variable
    _database_name = "vaccination_info.db"

    # Empty Constructor
    def __init__(self):
        pass

    # Creating Database and necessary Tables with relational constraints
    # public method
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
        db_connection.commit()
        db_cursor.close()
        db_connection.close()

    # public method
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
        db_cursor.close()
        db_connection.close()
        self.__normalize_data()

    # private method
    def __normalize_data(self):
        self.__distribute_data_between_relational_tables()
        self.__update_nan_values()

    # private method
    def __distribute_data_between_relational_tables(self):
        db_connection = sqlite3.connect(self._database_name)
        db_cursor = db_connection.cursor()
        db_cursor.execute(
            "insert into countries(name, iso_code) select country, iso_code from vaccin_covid_csv group by country")
        db_connection.commit()
        db_cursor.execute(
            "insert into vaccines(name, source, source_website) select vaccines, source_name , source_website  from vaccin_covid_csv group by vaccines")
        db_connection.commit()
        # Collecting all the country code and ids in a dictionary so that we don't have to srearch the database each
        # time we update a row
        countries = self.__get_country_ids()
        # Collecting all the vaccine name and ids in a dictionary so that we don't have to srearch the database each
        # time we update a row
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
                       row[3] if row[3] else 0,
                       row[4] if row[4] else 0,
                       row[5] if row[5] else 0,
                       row[6] if row[6] else 0,
                       row[7]  if row[7] else 0,
                       row[8]  if row[8] else 0,
                       row[9]  if row[9] else 0,
                       row[10]  if row[10] else 0,
                       row[11]  if row[11] else 0
                       ]
            normalized_db_cursor.execute("""insert into vaccine_info(country_id, vaccine_id, date, total_vaccinations, people_vaccinated,
                             people_fully_vaccinated, daily_vaccinations_raw, daily_vaccinations, total_vaccinations_per_hundred, 
                             people_vaccinated_per_hundred, people_fully_vaccinated_per_hundred, 
                             daily_vaccinations_per_million ) values(?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?)""", columns)
        normalized_db_connection.commit()
        normalized_db_cursor.close()
        normalized_db_connection.close()
        db_cursor.close()
        db_connection.close()


    # private method
    def __update_nan_values(self):
        countries = self.__get_country_ids()
        iso_codes = self.__get_country_iso_code_list()
        while iso_codes:
            iso_code = iso_codes.pop()
            records = self.__get_country_based_rows(countries, iso_code)
            previous_record = records[0]
            db_connection = sqlite3.connect(self._database_name)
            db_cursor = db_connection.cursor()
            for i in range(1, len(records)):
                current_record  = records[i]
                updated_record = []
                for x in range(3, 12):
                    if current_record[x] == 0:
                        updated_record.append(previous_record[x])
                    else:
                        updated_record.append(current_record[x])
                self.__update_nan_value_fixed_row(db_cursor, current_record[0], current_record[1], current_record[2], updated_record)
                previous_record = []
                previous_record.append(current_record[0])
                previous_record.append(current_record[1])
                previous_record.append(current_record[2])
                for data in updated_record:
                    previous_record.append(data)
            db_connection.commit()
            db_cursor.close()
            db_connection.close()

    def __update_nan_value_fixed_row(self, db_cursor, country_id, vaccine_id, date, new_data):
        new_data.append(country_id)
        new_data.append(vaccine_id)
        new_data.append(date)
        db_cursor.execute("""UPDATE vaccine_info SET total_vaccinations = ?, people_vaccinated=?, people_fully_vaccinated=?,
        daily_vaccinations_raw = ?, daily_vaccinations = ?, total_vaccinations_per_hundred = ?, people_vaccinated_per_hundred = ?,
        people_fully_vaccinated_per_hundred = ? , daily_vaccinations_per_million = ? WHERE country_id = ? AND vaccine_id = ? AND date = ?
        """, new_data)

    def __get_country_iso_code_list(self):
        db_connection = sqlite3.connect(self._database_name)
        db_cursor = db_connection.cursor()
        db_cursor.execute("select iso_code from countries")
        records = db_cursor.fetchall()
        iso_codes = []
        for row in records:
            iso_codes.append(row[0])
        db_cursor.close()
        db_connection.close()
        return iso_codes;


    # private method
    def __get_country_based_rows(self, countries, iso_code):
        db_connection = sqlite3.connect(self._database_name)
        db_cursor = db_connection.cursor()
        db_cursor.execute("select * from vaccine_info where country_id = ? order by date asc", [countries[iso_code]])
        records = db_cursor.fetchall()
        rows = []
        for row in records:
            rows.append(row)
        db_cursor.close()
        db_connection.close()
        return rows


    # private method
    def __get_country_ids(self):
        db_connection = sqlite3.connect(self._database_name)
        db_cursor = db_connection.cursor()
        db_cursor.execute("select iso_code, id from countries")
        countries = {}
        records = db_cursor.fetchall()
        for row in records:
            countries[row[0]] = row[1]
        db_cursor.close()
        db_connection.close()
        return countries

    # private method
    def __get_vaccine_ids(self):
        db_connection = sqlite3.connect(self._database_name)
        db_cursor = db_connection.cursor()
        db_cursor.execute("select name, id from vaccines")
        vaccines = {}
        records = db_cursor.fetchall()
        for row in records:
            vaccines[row[0]] = row[1]
        db_cursor.close()
        db_connection.close()
        return vaccines

    # public method
    def clean_data(self):
        db_connection = sqlite3.connect(self._database_name)
        db_cursor = db_connection.cursor()
        db_cursor.execute("DELETE FROM vaccin_covid_csv")
        db_connection.commit()
        db_cursor.execute("DELETE FROM countries")
        db_connection.commit()
        db_cursor.execute("DELETE FROM vaccines")
        db_connection.commit()
        db_cursor.execute("DELETE FROM vaccine_info")
        db_connection.commit()
        db_cursor.close()
        db_connection.close()

if __name__ == "__main__":
    data_manager = DataManager()
    data_manager.create_database()
    data_manager.clean_data()
    data_manager.seed_database('vaccin_covid.csv')
    
    
    
