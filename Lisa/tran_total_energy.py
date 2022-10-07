
from total_energy import import_data

import pandas as pd

def transform_data_renewables():
    
    tf_energy_data = import_data()
    
    tf_energy_data = tf_energy_data.rename(columns={
        "Unnamed: 0" : "IDs",
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
    
    tf_energy_data['IDs'] = range(0, len(tf_energy_data) )

    tf_energy_data = tf_energy_data.replace(to_replace = ['Wind','Solar Thermal and Photovoltaic', 
                                                 'Hydroelectric Conventional', 'Geothermal', 
                                                 'Wood and Wood Derived Fuels', 'Other Biomass', 
                                                 'Pumped Storage'],value = 'Renewable energy')
    tf_energy_data = tf_energy_data.groupby(['IDs','year','month','state','producer','source'],
                                            as_index=False).agg({'generated': 'sum'})
    
    return tf_energy_data

def transform_data_all():
    
    tf_energy_data = import_data()

    tf_energy_data = tf_energy_data.rename(columns={
        "Unnamed: 0" : "IDs",
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
    tf_energy_data['IDs'] = range(0, len(tf_energy_data))
    
    return tf_energy_data
