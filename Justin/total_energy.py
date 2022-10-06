
import pandas as pd

def import_data():
    #csv_file = "/Users/justinbaytosh/Desktop/coding/netflix/advanced/midterm-project/Justin/renewable_energy.csv"
    csv_file = "renewable_energy.csv"
    energy = pd.read_csv(csv_file)
    return energy
