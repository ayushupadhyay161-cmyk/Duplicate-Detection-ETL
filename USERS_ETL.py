# Duplicate-Detection-ETL

import os 
import pandas as pd 
from datetime import date

class USERS_ETL():
    def __init__(self, filename):
        self.filename = filename
        self.df = None 

    def extract(self):
        if not os.path.exists(self.filename):
            print("File Not Found")
            return
        self.df = pd.read_csv(self.filename)
        print("Data Extracted Successfully")

    def transform(self):
        if self.df is None:
            print("No Data To Transform")
            return 
        self.df["created_at"] = pd.to_datetime(self.df["created_at"], dayfirst=True, errors="coerce")
        self.df = self.df.sort_values(by="created_at", ascending = False)
        self.df = self.df.drop_duplicates(subset="email", keep="first")
        print("Duplicate Email's Removed")

    def load(self, output_path):
        self.df.to_csv(output_path, index=False)
        print("Data Loaded Successfully")

    def run(self):
        self.extract()
        self.transform()

        output_path = os.path.join(
            os.path.dirname(self.filename),
            "users_cleaned.csv"
       )
        self.load(output_path)

if __name__ == "__main__":
    file_path = "C:/Users/Admin/pandas_project/Rest API/users.csv"
    etl = USERS_ETL(file_path)
    etl.run()
