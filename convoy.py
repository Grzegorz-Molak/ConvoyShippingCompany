import re
import pandas as pd
import sqlite3 as sql
from lxml import etree


class Data:
    def __init__(self):
        self.file_name = input("Input file name\n")
        self.raw_file_name = self.get_raw_file_name()
        self.my_df = self.process_file()
        if self.file_name.split('.')[-1] != "s3db":
            self.save_to_db()

    def get_raw_file_name(self):
        return (self.file_name.replace('.'+self.file_name.split('.')[-1], "")).replace("[CHECKED]", "")

    def process_file(self):
        if self.file_name.split('.')[-1] == "s3db":
            return self.select_from_db()
        else:
            if "[CHECKED].csv" not in self.file_name:
                if self.file_name.split('.')[-1] == "xlsx":
                    self.file_name = self.xlsx_to_csv()
                return self.repair_data(pd.read_csv(self.file_name))
            else:
                return pd.read_csv(self.file_name)

    def xlsx_to_csv(self):
        my_df = pd.read_excel(self.file_name, sheet_name="Vehicles", dtype=str)
        my_df.to_csv(self.file_name.replace(".xlsx", ".csv"), index=False, header=True)

        if my_df.shape[0] == 0:
            pass
        elif my_df.shape[0] == 1:
            print(f"{my_df.shape[0]} line was added to {self.file_name.replace('.xlsx', '.csv')}")
        else:
            print(f"{my_df.shape[0]} lines were added to {self.file_name.replace('.xlsx', '.csv')}")

        return self.file_name.replace(".xlsx", ".csv")

    def repair_data(self, corrupted_df):
        count_repaired = 0
        repaired_df = corrupted_df
        for row in corrupted_df:
            for index, cell in enumerate(corrupted_df[row]):
                # print(cell)
                if not str(cell).isdigit():
                    repaired_df.loc[index, row] = re.sub("[^0-9]", "", cell)
                    count_repaired += 1
        self.file_name = self.raw_file_name+'[CHECKED].csv'
        self.print_message(count_repaired)

        repaired_df.to_csv(self.file_name, index=False, header=True)
        # print(repaired_df)
        return repaired_df

    def print_message(self, num_corrected):
        if num_corrected != 1:
            print(f"{num_corrected} cells were corrected in {self.file_name}")
        else:
            print(f"1 cell was corrected in {self.file_name}")

    def save_to_db(self):
        def generate_score(row):
            route_length = 450
            fuel_consumed = int(row["fuel_consumption"])*(route_length/100)
            pit_stops = fuel_consumed//int(row["engine_capacity"])
            result = 0
            if pit_stops == 1:
                result += 1
            elif pit_stops == 0:
                result += 2
            if fuel_consumed <= 230:
                result += 2
            else:
                result += 1
            if int(row["maximum_load"]) >= 20:
                result += 2
            return result

        self.file_name = self.raw_file_name+".s3db"
        conn = sql.connect(self.file_name)
        cursor = conn.cursor()
        headers = list(self.my_df.columns)
        scores = self.my_df.apply(generate_score, axis=1)
        create_db = f'''CREATE TABLE IF NOT EXISTS convoy (
                        {headers[0]} int PRIMARY KEY,
                        {headers[1]} int NOT NULL, 
                        {headers[2]} int NOT NULL, 
                        {headers[3]} int NOT NULL,
                        score int NOT NULL
                        );'''
        cursor.execute(create_db)
        for index in range(self.my_df.shape[0]):
            insert_db = f'''INSERT or REPLACE INTO convoy({','.join(headers)},score 
                    ) VALUES  ({','.join(map(str, self.my_df.iloc[index]))},{scores[index]}
                    );'''
            cursor.execute(insert_db)

        if self.my_df.shape[0] != 1:
            print(f"{self.my_df.shape[0]} records were inserted into {self.file_name}")
        else:
            print(f"1 record was inserted into {self.file_name}")

        conn.commit()
        conn.close()

    def select_from_db(self):
        self.file_name = self.raw_file_name+".s3db"
        conn = sql.connect(self.file_name)
        cursor = conn.cursor()
        headers = (list(elem[1] for elem in cursor.execute("PRAGMA table_info(convoy);").fetchall()))
        headers.pop()
        data_from_db = cursor.execute("SELECT * FROM convoy").fetchall()
        conn.commit()
        conn.close()
        json = []
        xml = []
        for row in data_from_db:
            score = row[-1]
            if score > 3:
                json.append(row[0:-1])
            else:
                xml.append(row[0:-1])
        # print(scores)
        # print(result)
        return [pd.DataFrame(json, columns=headers), pd.DataFrame(xml, columns=headers)]

    def save_to_json(self, df_json):
        self.file_name = self.raw_file_name+".json"
        to_save = df_json.to_json(orient="records")
        to_save = '{"convoy":' + to_save + "}"
        with open(self.file_name, "w") as file_json:
            file_json.write(to_save)
        if df_json.shape[0] != 1:
            print(f"{df_json.shape[0]} vehicles were saved  into {self.file_name}")
        else:
            print(f"1 vehicle was saved into {self.file_name}")

    def save_to_xml(self, df_xml):
        def row_to_xml(row):
            xml = ['<vehicle>']
            for index, column in enumerate(row.index):
                xml.append('<{0}>{1}</{0}>'.format(column, row.iloc[index]))
            xml.append("</vehicle>")
            return '\n'.join(xml)
        self.file_name = self.raw_file_name+".xml"
        res = '\n'.join( df_xml.apply(row_to_xml, axis=1))
        res = '\n'.join(["<convoy>", res, "</convoy>"])
        with open(self.file_name, "w") as xml_file:
            xml_file.write(res)
        if df_xml.shape[0] != 1:
            print(f"{ df_xml.shape[0]} vehicles were saved  into {self.file_name}")
        else:
            print(f"1 vehicle was saved into {self.file_name}")

    def save_to_json_xml(self, df):
        self.save_to_json(df[0])
        self.save_to_xml(df[1])


data = Data()
data.save_to_json_xml(data.select_from_db())

