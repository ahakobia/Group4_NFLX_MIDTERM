
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

    season= tf_energy_data['month'].apply(
        lambda x: 'Winter' if x in (12,1,2) else 'Spring' 
            if x in (3,4,5) else 'Summer' 
            if x in (6,7,8) else 'Autumn')

    tf_energy_data['producer'] = tf_energy_data['producer'].apply(lambda x: x.replace(',','/'))
    tf_energy_data = tf_energy_data[tf_energy_data.state != 'US-TOTAL']
    tf_energy_data = tf_energy_data[tf_energy_data.producer != 'Total Electric Power Industry']
    tf_energy_data = tf_energy_data[tf_energy_data.source != 'Total']
    
    tf_energy_data.insert(5, 'season', season)
    
    tf_energy_data = tf_energy_data.reset_index(drop = True)

    first_column = range(0, len(tf_energy_data) )
    tf_energy_data.insert(0, 'IDs', first_column)
    return tf_energy_data

def transform_data2():
    tf_energy_data = import_data().drop(columns=['Unnamed: 0'])
    
    tf_energy_data = tf_energy_data.rename(columns={
    "YEAR": "year", 
    "MONTH": "month",
    "STATE": "state",
    "TYPE OF PRODUCER": "producer",
    "ENERGY SOURCE": "source",
    "GENERATION (Megawatthours)": "generated"})

    season = tf_energy_data['month'].apply(
        lambda x: 'Winter' if x in (12,1,2) else 'Spring' 
            if x in (3,4,5) else 'Summer' 
            if x in (6,7,8) else 'Autumn')

    tf_energy_data['producer'] = tf_energy_data['producer'].apply(lambda x: x.replace(',','/'))
    tf_energy_data = tf_energy_data[tf_energy_data.state != 'US-TOTAL']
    tf_energy_data = tf_energy_data[tf_energy_data.producer != 'Total Electric Power Industry']
    tf_energy_data = tf_energy_data[tf_energy_data.source != 'Total']
    
    tf_energy_data = tf_energy_data.replace(to_replace = ['Wind','Solar Thermal and Photovoltaic', 
                                                     'Hydroelectric Conventional', 'Geothermal', 
                                                     'Wood and Wood Derived Fuels', 'Other Biomass', 
                                                     'Pumped Storage'],value = 'Renewable energy')
    tf_energy_data.insert(4, 'season', season)
    tf_energy_data = tf_energy_data.groupby(['year','month','state','producer','source','season'],
                                                as_index=False).agg({'generated': 'sum'})
    
    tf_energy_data = tf_energy_data.reset_index(drop = True)

    first_column = range(0, len(tf_energy_data) )
    tf_energy_data.insert(0, 'ids', first_column)
    return tf_energy_data
