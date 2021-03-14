Table tourist_vists {
  id SERIAL [pk, increment]
  airport_id VARCHAR(5) [ref: > airports.id, not null]
  city_id VARCHAR(40) [ref: > usa_cities.id, not null]
  arrival_date datetime [ref: > weather.date, not null]
  depart_date datetime [not null]
  mode_of_travel VARCHAR(20)
  airline VARCHAR(40)
  visa_post VARCHAR(40)
  visa_type VARCHAR(40)
  tourist_id INTEGER [ref: > tourists.id, not null]
}

Table weather {
  date DATE [pk]
  avg_daily_temp DECIMAL
  avg_daily_temp_uncertainty DECIMAL
}

Table usa_cities {
  id SERIAL [pk, increment]
  name VARCHAR(80) 
  median_age DECIMAL
  male_pop INTEGER
  female_pop INTEGER
  total_pop INTEGER
  num_veterans INTEGER
  foreign_born_pop INTEGER
  avg_house_size DECIMAL
}

Table airports {
  id VARCHAR(5) [pk]
  name VARCHAR(40)
  municipality VARCHAR(40)
  state VARCHAR(2)
  elevation_ft DECIMAL
  longitude DECIMAL
  latitude DECIMAL
}

Table tourists {
  id SERIAL [pk, increment]
  birth_year INTEGER
  citizen_id VARCHAR(20) [not null]
  cntry_citizenship VARCHAR(40) [not null]
  gender VARCHAR(2)
}