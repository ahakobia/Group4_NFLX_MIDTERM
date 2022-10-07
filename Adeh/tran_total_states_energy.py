
from total_states_energy import import_data

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
    tf_energy_data = tf_energy_data[tf_energy_data.state != 'US-TOTAL']
    tf_energy_data = tf_energy_data[tf_energy_data.producer != 'Total Electric Power Industry']
    tf_energy_data = tf_energy_data[tf_energy_data.source != 'Total']
    
    tf_energy_data = tf_energy_data.reset_index(drop = True)

    first_column = range(0, len(tf_energy_data) )
    tf_energy_data.insert(0, 'IDs', first_column)
    return tf_energy_data
