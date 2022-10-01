
from total_energy import import_data

import pandas as pd

def transform_data():
    
    tf_energy_data = import_data().drop(columns=['Unnamed: 0'])
    
    tf_energy_data = tf_energy_data.rename(columns={
        "YEAR": "year", 
        "MONTH": "month",
        "STATE": "state",
        "TYPE OF PRODUCER": "producer",
        "ENERGY SOURCE": "source",
        "GENERATION (Megawatthours)": "generated"})


    tf_energy_data['producer'] = tf_energy_data['producer'].apply(lambda x: x.replace(',','/'))
    tf_energy_data = tf_energy_data[tf_energy_data.state == 'US-TOTAL']
    tf_energy_data = tf_energy_data[tf_energy_data.producer == 'Total Electric Power Industry']
    tf_energy_data = tf_energy_data[tf_energy_data.source == 'Total']
    
    return tf_energy_data.reset_index(drop = True)
