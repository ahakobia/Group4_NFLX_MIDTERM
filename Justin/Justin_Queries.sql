DROP TABLE us_energy;

CREATE TABLE us_energy (
	id SERIAL PRIMARY KEY,
	row INT,
	year INT,
	month INT,
	state VARCHAR(8),
	producer VARCHAR(50),
	source VARCHAR(50),
	energy FLOAT
);

COPY us_energy (row, year, month, state, producer, source, energy) FROM '/Users/justinbaytosh/Desktop/coding/netflix/advanced/midterm-project/Resources/organised_Gen.csv'
DELIMITER ','
CSV HEADER;

CREATE VIEW renewable_energy AS
SELECT year, producer, SUM(energy) AS total_generated FROM us_energy
WHERE year >= 2011 AND year < 2022 AND (source='Wind' OR source='Solar Thermal and Photovoltaic' OR source='Hydroelectric Conventional' OR source='Geothermal' OR source='Wood and Wood Derived Fuels' OR source='Other Biomass' OR source='Pumped Storage')
GROUP BY year, producer
ORDER BY year DESC, producer DESC;

COPY (SELECT * FROM renewable_energy) TO '/Users/justinbaytosh/Desktop/coding/netflix/advanced/midterm-project/Justin/renewable_energy.csv' 
DELIMITER ',' 
CSV HEADER;