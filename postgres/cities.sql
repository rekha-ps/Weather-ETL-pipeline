CREATE TABLE IF NOT EXISTS cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    lat FLOAT NOT NULL,
    lon FLOAT NOT NULL,
    region VARCHAR(50),        -- e.g., England, Scotland, Wales, Northern Ireland
    active BOOLEAN DEFAULT TRUE
);

-- England
INSERT INTO cities (city_name, lat, lon, region) VALUES
('London', 51.5074, -0.1278, 'England'),
('Birmingham', 52.4862, -1.8904, 'England'),
('Manchester', 53.4808, -2.2426, 'England'),
('Liverpool', 53.4084, -2.9916, 'England'),
('Leeds', 53.8008, -1.5491, 'England'),
('Sheffield', 53.3811, -1.4701, 'England'),
('Bristol', 51.4545, -2.5879, 'England'),
('Nottingham', 52.9548, -1.1581, 'England'),
('Leicester', 52.6369, -1.1398, 'England'),
('Coventry', 52.4068, -1.5197, 'England'),
('Oxford', 51.7520, -1.2577, 'England'),
('Cambridge', 52.2053, 0.1218, 'England'),
('York', 53.9590, -1.0815, 'England'),
('Bath', 51.3758, -2.3599, 'England'),
('Canterbury', 51.2798, 1.0837, 'England'),
('Milton Keynes', 52.0406, -0.7594, 'England'),
('Wolverhampton', 52.5862, -2.1288, 'England'),
('Brighton & Hove', 50.8225, -0.1372, 'England'),
('Exeter', 50.7184, -3.5339, 'England'),
('Southampton', 50.9097, -1.4044, 'England'),
('Portsmouth', 50.8198, -1.0880, 'England'),
('Norwich', 52.6309, 1.2974, 'England'),
('Newcastle-upon-Tyne', 54.9784, -1.6174, 'England'),
('Preston', 53.7632, -2.7031, 'England'),
('Sunderland', 54.9069, -1.3838, 'England'),

-- Scotland
('Glasgow', 55.8642, -4.2518, 'Scotland'),
('Edinburgh', 55.9533, -3.1883, 'Scotland'),
('Aberdeen', 57.1497, -2.0943, 'Scotland'),
('Dundee', 56.4620, -2.9707, 'Scotland'),
('Inverness', 57.4778, -4.2247, 'Scotland'),
('Perth', 56.3969, -3.4370, 'Scotland'),
('Stirling', 56.1165, -3.9369, 'Scotland'),

-- Wales
('Cardiff', 51.4816, -3.1791, 'Wales'),
('Swansea', 51.6214, -3.9436, 'Wales'),
('Newport', 51.5842, -2.9977, 'Wales'),
('Wrexham', 53.0465, -2.9913, 'Wales'),

-- Northern Ireland
('Belfast', 54.5973, -5.9301, 'Northern Ireland'),
('Derry', 54.9966, -7.3086, 'Northern Ireland'),
('Lisburn', 54.5125, -6.0416, 'Northern Ireland'),
('Armagh', 54.3506, -6.6528, 'Northern Ireland'),
('Bangor', 54.6600, -5.6700, 'Northern Ireland');