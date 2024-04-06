-- Create the old non-optimized schema

CREATE OR REPLACE PROCEDURE create_populate_schema()
LANGUAGE SQL 
AS $$

-- Drop tables before updating/populating them

DROP TABLE IF EXISTS Vehicles;
DROP TABLE IF EXISTS Crimes;
DROP TABLE IF EXISTS Officer;
DROP TABLE IF EXISTS PhoneCalls;
DROP TABLE IF EXISTS Phones;
DROP TABLE IF EXISTS Current_address;
DROP TABLE IF EXISTS Involved_in;
DROP TABLE IF EXISTS Occurred_at;
DROP TABLE IF EXISTS Has_phone;
DROP TABLE IF EXISTS Family_rel;
DROP TABLE IF EXISTS Caller;
DROP TABLE IF EXISTS Called;
DROP TABLE IF EXISTS Investigated_by;
DROP TABLE IF EXISTS People;
DROP TABLE IF EXISTS Locations;

CREATE TABLE IF NOT EXISTS Locations (
    id INT PRIMARY KEY,
    latitude FLOAT,
    longitude FLOAT,
    address TEXT,
    postcode TEXT
);

CREATE TABLE IF NOT EXISTS Crimes (
    id INT PRIMARY KEY,
    date DATE,
    type TEXT,
    last_outcome TEXT,
    note TEXT,
    charge TEXT
);

CREATE TABLE IF NOT EXISTS Officer (
    id INT PRIMARY KEY,
    badge_no TEXT,
    rank TEXT,
    name TEXT,
    surname TEXT
);

CREATE TABLE IF NOT EXISTS People (
    id INT PRIMARY KEY,
    name TEXT,
    surname TEXT,
    age INT,
    nhs_no TEXT
);

CREATE TABLE IF NOT EXISTS PhoneCalls (
    id INT PRIMARY KEY,
    call_date DATE,
    call_time TIME,
    call_duration INT,
    call_type TEXT
);

CREATE TABLE IF NOT EXISTS Phones (
    id INT PRIMARY KEY,
    phoneNo TEXT
);


CREATE TABLE IF NOT EXISTS Vehicles (
    id INT PRIMARY KEY,
    make TEXT,
    model TEXT,
    year INT,
    reg TEXT
);

CREATE TABLE IF NOT EXISTS Current_address (
    person_id INT,
    location_id INT
);

CREATE TABLE IF NOT EXISTS Investigated_by (
    crime_id INT,
    officer_id INT
);

CREATE TABLE IF NOT EXISTS Involved_in (
    vehicle_id INT,
    crime_id INT
);

CREATE TABLE IF NOT EXISTS Occurred_at (
    location_id INT,
    crime_id INT
);


CREATE TABLE IF NOT EXISTS Has_phone (
    person_id INT,
    phone_id INT
);

CREATE TABLE IF NOT EXISTS Family_rel (
    person_id1 INT,
    person_id2 INT
);

CREATE TABLE IF NOT EXISTS Caller (
    call_id INT,
    phone_id INT
);

CREATE TABLE IF NOT EXISTS Called (
    call_id INT,
    phone_id INT
);


-- Load data in DataFrame

SET datestyle = "ISO, DMY";

COPY Locations(id, latitude, postcode, longitude, address)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.nodes.Location.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY Crimes(id, date, type, last_outcome, note, charge)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.nodes.Crime.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY Officer(id, badge_no, rank, name, surname)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.nodes.Officer.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY People(id, surname,  nhs_no, name, age)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.nodes.Person.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY PhoneCalls(id, call_duration, call_time, call_date, call_type)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.nodes.PhoneCall.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY Phones(id, phoneNo)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.nodes.Phone.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY Vehicles(id, model, reg, make, year)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.nodes.Vehicle.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY Current_address(person_id, location_id)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.relationships.CURRENT_ADDRESS.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY Involved_in(vehicle_id, crime_id)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.relationships.INVOLVED_IN.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY Occurred_at(crime_id, location_id)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.relationships.OCCURRED_AT.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY Has_phone(person_id, phone_id)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.relationships.HAS_PHONE.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY Family_rel(person_id1, person_id2)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.relationships.FAMILY_REL.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY Caller(call_id, phone_id)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.relationships.CALLER.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY Called(call_id, phone_id)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.relationships.CALLED.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY Investigated_by(crime_id, officer_id)
FROM 'C:\Users\Public\pole-data-importer\crime-investigation.relationships.INVESTIGATED_BY.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

$$;

-- Modify and optimize the old schema

CREATE OR REPLACE PROCEDURE optimize_types_attributes()
LANGUAGE SQL 
AS $$

-- Crimes

ALTER TABLE Crimes
ADD COLUMN IF NOT EXISTS officer_id INT,
ADD COLUMN IF NOT EXISTS location_id INT,
ALTER COLUMN type TYPE VARCHAR(50),
ALTER COLUMN last_outcome TYPE VARCHAR(255),
ALTER COLUMN note TYPE VARCHAR(255),
ALTER COLUMN charge TYPE VARCHAR(255),
DROP CONSTRAINT IF EXISTS investigated_by,
DROP CONSTRAINT IF EXISTS occurred_at,
ADD CONSTRAINT investigated_by FOREIGN KEY (officer_id) REFERENCES Officer(id),
ADD CONSTRAINT occurred_at FOREIGN KEY (location_id) REFERENCES Locations(id);

-- Vehicles

ALTER TABLE Vehicles
ADD COLUMN IF NOT EXISTS crime_id INT,
ALTER COLUMN make TYPE VARCHAR(50),
ALTER COLUMN model TYPE VARCHAR(50),
ALTER COLUMN reg TYPE VARCHAR(50),
DROP CONSTRAINT IF EXISTS involved_in,
ADD CONSTRAINT involved_in FOREIGN KEY (crime_id) REFERENCES Crimes(id);

-- Officer

ALTER TABLE Officer
ALTER COLUMN badge_no TYPE VARCHAR(20),
ALTER COLUMN rank TYPE VARCHAR(30),
ALTER COLUMN name TYPE VARCHAR(100),
ALTER COLUMN surname TYPE VARCHAR(100),
ALTER COLUMN badge_no SET NOT NULL,
ALTER COLUMN rank SET NOT NULL,
ALTER COLUMN name SET NOT NULL,
ALTER COLUMN surname SET NOT NULL,
ADD UNIQUE (badge_no);

-- People

ALTER TABLE People
ADD COLUMN IF NOT EXISTS address_id INT,
ALTER COLUMN name TYPE VARCHAR(100),
ALTER COLUMN surname TYPE VARCHAR(100),
ALTER COLUMN nhs_no TYPE VARCHAR(20),
ALTER COLUMN age TYPE SMALLINT,
ALTER COLUMN name SET NOT NULL,
ALTER COLUMN surname SET NOT NULL,
ADD UNIQUE (nhs_no),
DROP CONSTRAINT IF EXISTS age_bound,
DROP CONSTRAINT IF EXISTS current_address,
ADD CONSTRAINT age_bound CHECK (age >= 0 AND age <= 120),
ADD CONSTRAINT current_address FOREIGN KEY (address_id) REFERENCES Locations(id);

-- Family_Rel

ALTER TABLE Family_rel
DROP CONSTRAINT IF EXISTS p1,
DROP CONSTRAINT IF EXISTS p2,
ADD CONSTRAINT p1 FOREIGN KEY (person_id1) REFERENCES People(id),
ADD CONSTRAINT p2 FOREIGN KEY (person_id2) REFERENCES People(id);

-- PhoneCalls

ALTER TABLE PhoneCalls
ADD COLUMN IF NOT EXISTS from_phone_id INT,
ADD COLUMN IF NOT EXISTS to_phone_id INT,
ALTER COLUMN call_date TYPE DATE,
ALTER COLUMN call_duration TYPE SMALLINT,
ALTER COLUMN call_time TYPE TIME,
DROP CONSTRAINT IF EXISTS caller,
DROP CONSTRAINT IF EXISTS called,
ADD CONSTRAINT caller FOREIGN KEY (from_phone_id) REFERENCES Phones(id),
ADD CONSTRAINT called FOREIGN KEY (to_phone_id) REFERENCES Phones(id);

-- Phones 

ALTER TABLE Phones
ADD COLUMN IF NOT EXISTS owner_id INT,
ALTER COLUMN phoneNo TYPE VARCHAR(20),
DROP CONSTRAINT IF EXISTS has_phone,
ADD CONSTRAINT has_phone FOREIGN KEY (owner_id) REFERENCES People(id);

-- Locations

ALTER TABLE Locations
ALTER COLUMN latitude TYPE DECIMAL(9,6),
ALTER COLUMN longitude TYPE DECIMAL(9,6),
ALTER COLUMN address TYPE VARCHAR(255),
ALTER COLUMN postcode TYPE VARCHAR(20),
ALTER COLUMN latitude SET NOT NULL,
ALTER COLUMN longitude SET NOT NULL,
ALTER COLUMN address SET NOT NULL,
ALTER COLUMN postcode SET NOT NULL;

-- Create indexes

CREATE INDEX IF NOT EXISTS locations_index ON Locations(id);
CREATE INDEX IF NOT EXISTS crimes_index ON Crimes(id);
CREATE INDEX IF NOT EXISTS officer_index ON Officer(id);
CREATE INDEX IF NOT EXISTS people_index ON People(id);
CREATE INDEX IF NOT EXISTS vehicles_index ON Vehicles(id);
CREATE INDEX IF NOT EXISTS phones_index ON Phones(id);
CREATE INDEX IF NOT EXISTS phonecalls_index ON PhoneCalls(id);

$$;

-- Populate the DB

CALL create_populate_schema();
call optimize_types_attributes();

UPDATE Crimes
SET officer_id = (SELECT officer_id FROM investigated_by WHERE crime_id = Crimes.id),
	location_id = (SELECT location_id FROM occurred_at WHERE crime_id = Crimes.id);

-- Vehicles
	
UPDATE Vehicles
SET crime_id = (SELECT crime_id FROM involved_in WHERE vehicle_id = Vehicles.id);

-- People

UPDATE People
SET address_id = (SELECT location_id FROM current_address WHERE person_id = People.id);

-- PhoneCalls

UPDATE PhoneCalls
SET to_phone_id = (SELECT phone_id FROM called WHERE call_id = PhoneCalls.id),
	from_phone_id = (SELECT phone_id FROM caller WHERE call_id = PhoneCalls.id);

-- Phones

UPDATE Phones
SET owner_id = (SELECT person_id FROM has_phone WHERE phone_id = Phones.id);

-- Drop old relation tables from non-optimized schema

DROP TABLE has_phone;
DROP TABLE involved_in;
DROP TABLE occurred_at;
DROP TABLE current_address;
DROP TABLE investigated_by;
DROP TABLE called;
DROP TABLE caller;


-- Queries 

-- Q1

CREATE VIEW OfficerCrimeCount AS
SELECT c.officer_id,
       COUNT(DISTINCT c.id) AS TotCrimes
FROM crimes c
GROUP BY c.officer_id
HAVING COUNT(DISTINCT c.id) > 30;

SELECT o.surname, o.badge_no, o.rank
FROM officer o
INNER JOIN OfficerCrimeCount occ ON o.id = occ.officer_id
ORDER BY o.surname;


-- Q2

SELECT
    l.postcode AS PostCode,
    COUNT(c.id) AS CrimeCount
FROM locations l
JOIN Crimes c ON c.location_id = l.id
GROUP BY l.postcode
ORDER BY CrimeCount DESC
LIMIT 10;


-- Q3

CREATE VIEW DailyCrimeChange AS
SELECT
    v.area,
    v.date,
    v.crime_count - LAG(v.crime_count, 1, v.crime_count) OVER (PARTITION BY v.area ORDER BY v.date) AS daily_change
FROM (
    SELECT
		l.postcode AS area,
        c.date,
        COUNT(DISTINCT c.id) AS crime_count
    FROM Crimes c
	JOIN Locations l ON c.location_id = l.id
    GROUP BY area, c.date) AS v
;

SELECT
    d.area,
    ROUND(AVG(ABS(d.daily_change)), 2) AS avg_daily_change
FROM DailyCrimeChange d
GROUP BY d.area
ORDER BY avg_daily_change DESC
LIMIT 10;


-- Q4

SELECT
    v.make AS VehicleBrand,
    v.model AS VehicleModel,
    COUNT(DISTINCT v.reg) AS VehicleCount,
    COUNT(DISTINCT v.crime_id) AS CrimeCount
FROM
    Vehicles v
GROUP BY
    v.make, v.model
ORDER BY
    VehicleCount DESC;


-- Q5

SELECT
    c.type AS CrimeType,
    AVG(l.latitude) AS CentroidLatitude,
    AVG(l.longitude) AS CentroidLongitude,
    ROUND((111.111 * STDDEV(l.latitude))::numeric, 2) AS StdLatitude_km,
    ROUND((111.111 * COS(STDDEV(l.latitude)) * STDDEV(l.longitude))::numeric, 2) AS StdLongitude_km,
    COUNT(DISTINCT l.postcode) AS DifferentArea
FROM Crimes c
JOIN Locations l ON c.location_id = l.id
GROUP BY
    c.type
ORDER BY
    DifferentArea DESC;


-- Q7

CREATE VIEW OfficerCrimes AS
SELECT
    o.badge_no AS OfficerBadge,
    c.type AS CrimeType,
    COUNT(*) FILTER (WHERE c.last_outcome = 'Under investigation') AS NumUnresolved,
    COUNT(*) AS CrimeTypeFreq
FROM Crimes c
JOIN Officer o ON c.officer_id = o.id
GROUP BY o.badge_no, c.type;

CREATE VIEW RankedOfficerCrimes AS
SELECT
    OfficerBadge,
    CrimeType AS OfficerSpecialization,
    SUM(NumUnresolved) OVER (PARTITION BY OfficerBadge) AS TotalNumUnresolved,
    CrimeTypeFreq,
    ROW_NUMBER() OVER (PARTITION BY OfficerBadge ORDER BY CrimeTypeFreq DESC) AS rank
FROM OfficerCrimes;

SELECT
    OfficerBadge,
    OfficerSpecialization,
    TotalNumUnresolved
FROM RankedOfficerCrimes
WHERE rank = 1
ORDER BY TotalNumUnresolved DESC;


-- Q8

SELECT 
    l.postcode AS Area,
    COUNT(DISTINCT c.id) AS TotalCrimes,
    COUNT(DISTINCT p.id) AS TotalPeople,
    COUNT(DISTINCT CASE WHEN ph.id = pc_from.from_phone_id THEN pc_from.id ELSE NULL END) 
    + COUNT(DISTINCT CASE WHEN ph.id = pc_to.to_phone_id THEN pc_to.id ELSE NULL END) AS TotalTraffic
FROM 
    locations l
LEFT JOIN 
    crimes c ON c.location_id = l.id
JOIN 
    people p ON p.address_id = l.id
LEFT JOIN 
    phones ph ON p.id = ph.owner_id
LEFT JOIN 
    phonecalls pc_from ON ph.id = pc_from.from_phone_id
LEFT JOIN 
    phonecalls pc_to ON ph.id = pc_to.to_phone_id
GROUP BY 
    l.postcode
ORDER BY 
    TotalCrimes DESC, TotalTraffic DESC, TotalPeople DESC;


-- Q9

CREATE VIEW AggregatedCrimeOutcomes AS
SELECT
    type AS CrimeType,
    last_outcome AS LastOutcome,
    COUNT(*) AS Frequency,
    SUM(COUNT(*)) OVER (PARTITION BY type) AS TotalCases,
    ROW_NUMBER() OVER (PARTITION BY type ORDER BY COUNT(*) DESC) AS rn
FROM
    Crimes
GROUP BY
    type, last_outcome;

SELECT
    CrimeType,
    LastOutcome AS MostCommonOutcome,
    Frequency,
    TotalCases,
    ROUND((Frequency::DECIMAL / TotalCases) * 100, 2) AS Percentage
FROM
    AggregatedCrimeOutcomes
WHERE rn = 1
ORDER BY
    TotalCases DESC, CrimeType;