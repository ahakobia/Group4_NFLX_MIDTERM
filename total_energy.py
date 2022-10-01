
import pandas as pd

def import_data():
    csv_file = "Resources/organised_Gen.csv"
    energy = pd.read_csv(csv_file)
    return energy
