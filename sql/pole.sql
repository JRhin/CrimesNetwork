-- Define Database: POLE_CRIMES

DROP DATABASE IF EXISTS POLE_CRIMES;

CREATE DATABASE POLE_CRIMES
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

COMMENT ON DATABASE POLE_CRIMES
    IS 'Data Management Project a.y. 2023/24';


-- Create schema

CREATE TABLE Locations (
    id INT PRIMARY KEY,
    latitude FLOAT,
    longitude FLOAT,
    address TEXT,
    postcode TEXT
);

CREATE TABLE Crimes (
    id INT PRIMARY KEY,
    date DATE,
    type TEXT,
    last_outcome TEXT,
    note TEXT,
    charge TEXT
);

CREATE TABLE Officer (
    id INT PRIMARY KEY,
    badge_no TEXT,
    rank TEXT,
    name TEXT,
    surname TEXT
);

CREATE TABLE People (
    id INT PRIMARY KEY,
    name TEXT,
    surname TEXT,
    age INT,
    nhs_no TEXT
);

CREATE TABLE PhoneCalls (
    id INT PRIMARY KEY,
    call_date DATE,
    call_time TIME,
    call_duration INT,
    call_type TEXT
);

CREATE TABLE Phones (
    id INT PRIMARY KEY,
    phoneNo TEXT
);


CREATE TABLE Vehicles (
    id INT PRIMARY KEY,
    make TEXT,
    model TEXT,
    year INT,
    reg TEXT
);

CREATE TABLE Current_address (
    person_id INT,
    location_id INT
);

CREATE TABLE Investigated_by (
    crime_id INT,
    officer_id INT
);

CREATE TABLE Involved_in (
    vehicle_id INT,
    crime_id INT
);

CREATE TABLE Occurred_at (
    location_id INT,
    crime_id INT
);


CREATE TABLE Has_phone (
    person_id INT,
    phone_id INT
);

CREATE TABLE Family_rel (
    person_id1 INT,
    person_id2 INT
);

CREATE TABLE Caller (
    call_id INT,
    phone_id INT
);

CREATE TABLE Called (
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


-- Queries

-- Q1

SELECT o.surname, o.badge_no, o.rank
FROM officer o
WHERE EXISTS (
	SELECT ib.officer_id,
		COUNT(DISTINCT ib.crime_id) AS TotCrimes
	FROM investigated_by ib
	WHERE o.id = ib.officer_id
	GROUP BY ib.officer_id
	HAVING COUNT(DISTINCT ib.crime_id) > 30
)
ORDER BY o.badge_no;


-- Q2

SELECT
    l.postcode AS PostCode,
    COUNT(c.id) AS CrimeCount
FROM locations l
JOIN occurred_at o ON l.id = o.location_id
JOIN Crimes c ON o.crime_id = c.id
GROUP BY l.postcode
ORDER BY CrimeCount DESC
LIMIT 10;


-- Q3

SELECT
    d1.area,
    ROUND(AVG(ABS(d1.daily_change)), 2) AS avg_daily_change
FROM (
    SELECT
        d.area,
        d.date,
        d.crime_count - LAG(d.crime_count, 1, d.crime_count) OVER (PARTITION BY d.area ORDER BY d.date) AS daily_change
    FROM (
        SELECT
            l.postcode AS area,
            c.date,
            COUNT(DISTINCT c.id) AS crime_count
        FROM Crimes c
        JOIN Occurred_at o ON c.id = o.crime_id
        JOIN Locations l ON o.location_id = l.id
        GROUP BY l.postcode, c.date
    ) AS d
) AS d1
GROUP BY d1.area
ORDER BY avg_daily_change DESC
LIMIT 10;


-- Q4

SELECT
    v.make AS VehicleBrand,
    v.model AS VehicleModel,
    COUNT(DISTINCT v.reg) AS VehicleCount,
    COUNT(DISTINCT c.id) AS CrimeCount
FROM Vehicles v
JOIN Involved_in i ON v.id = i.vehicle_id
JOIN Crimes c ON i.crime_id = c.id
GROUP BY v.make, v.model
ORDER BY VehicleCount DESC;


-- Q5

SELECT
    c.type AS CrimeType,
    AVG(l.latitude) AS CentroidLatitude,
    AVG(l.longitude) AS CentroidLongitude,
    ROUND((111.111 * STDDEV(l.latitude))::numeric, 2) AS StdLatitude_km,
    ROUND((111.111 * COS(STDDEV(l.latitude)) * STDDEV(l.longitude))::numeric, 2) AS StdLongitude_km,
    COUNT(DISTINCT l.postcode) AS DifferentArea
FROM Crimes c
JOIN Occurred_at o ON c.id = o.crime_id
JOIN Locations l ON o.location_id = l.id
GROUP BY c.type
ORDER BY DifferentArea DESC;


-- Q6

SELECT
    p.name || ' ' || p.surname AS Person,
    p.nhs_no AS NHS_Number,
    ph.phoneNo AS Phone_Number,
    COUNT(DISTINCT pc.id) AS No_Fam_Calls
FROM Crimes c
JOIN Occurred_at o ON c.id = o.crime_id
JOIN Locations l ON o.location_id = l.id
JOIN Current_address ca ON l.id = ca.location_id
JOIN People p ON ca.person_id = p.id
JOIN Has_phone hp ON p.id = hp.person_id
JOIN Phones ph ON hp.phone_id = ph.id
JOIN (SELECT call_id, phone_id FROM Caller UNION ALL SELECT call_id, phone_id FROM Called) calls ON ph.id = calls.phone_id
JOIN PhoneCalls pc ON calls.call_id = pc.id AND pc.call_date >= c.date - INTERVAL '10 day' AND pc.call_date < c.date
JOIN Has_phone hp2 ON calls.phone_id = hp2.phone_id
WHERE
    c.id = 47413 AND
	hp2.person_id NOT IN (SELECT person_id2 AS Relative FROM family_rel WHERE person_id1 = p.id)
GROUP BY Person, NHS_Number, Phone_Number
ORDER BY No_Fam_Calls DESC;


-- Q7

SELECT
    OfficerBadge,
    OfficerSpecialization,
    TotalNumUnresolved
FROM (
    SELECT
        OfficerBadge,
        CrimeType AS OfficerSpecialization,
        SUM(NumUnresolved) OVER (PARTITION BY OfficerBadge) AS TotalNumUnresolved,
        CrimeTypeFreq,
        ROW_NUMBER() OVER (PARTITION BY OfficerBadge ORDER BY CrimeTypeFreq DESC) AS rank
    FROM (
		SELECT
			o.badge_no AS OfficerBadge,
			c.type AS CrimeType,
			COUNT(*) FILTER (WHERE c.last_outcome = 'Under investigation') AS NumUnresolved,
			COUNT(*) AS CrimeTypeFreq
		FROM Crimes c
		JOIN Investigated_by ib ON c.id = ib.crime_id
		JOIN Officer o ON ib.officer_id = o.id
		GROUP BY o.badge_no, c.type) AS OfficerCrimes) AS RankedOfficerCrimes
WHERE rank = 1
ORDER BY TotalNumUnresolved DESC;


-- Q8

SELECT l.postcode AS Area,
	   COUNT(DISTINCT c.id) AS TotalCrimes,
	   COUNT(DISTINCT p.id) AS TotalPeople,
	   COUNT(DISTINCT all_calls.call_id) AS TotalTraffic
FROM crimes AS c
LEFT JOIN occurred_at o ON o.crime_id = c.id
LEFT JOIN locations l ON o.location_id = l.id
JOIN current_address ca ON ca.location_id = l.id
JOIN people p ON p.id = ca.person_id
LEFT JOIN has_phone hp ON p.id = hp.person_id
LEFT JOIN (
	SELECT *
	FROM caller
	UNION
	SELECT *
	FROM called) AS all_calls ON hp.phone_id = all_calls.phone_id
GROUP BY l.postcode
ORDER BY TotalCrimes DESC, TotalTraffic DESC, TotalPeople DESC;


-- Q9

SELECT
    CrimeType,
    LastOutcome AS MostCommonOutcome,
    Frequency,
    TotalCases,
    ROUND((Frequency::DECIMAL / TotalCases) * 100, 2) AS Percentage
FROM (
    SELECT
        type AS CrimeType,
        last_outcome AS LastOutcome,
        COUNT(*) AS Frequency,
        SUM(COUNT(*)) OVER (PARTITION BY type) AS TotalCases,
        ROW_NUMBER() OVER (PARTITION BY type ORDER BY COUNT(*) DESC) AS rn
    FROM Crimes
    GROUP BY type, last_outcome) AS AggregatedOutcomes
WHERE rn = 1
ORDER BY TotalCases DESC, CrimeType;


-- Q10

SELECT hp.person_id AS PersonID,
    COUNT(DISTINCT ph.id) AS NumDistinctPhones,
	SUM(cl.CallDuration) AS TotalCallTime,
    ROUND(AVG(cl.CallDuration), 2) AS AvgCallDuration,
    COUNT(DISTINCT cl.CallID) AS TotCalls,
    COUNT(DISTINCT cl.CallDate) AS DistinctCallDate
FROM Has_phone hp
    LEFT JOIN Phones ph ON hp.phone_id = ph.id
    JOIN (
        SELECT cl.call_id AS CallID,
			   cl.phone_id AS PhoneID,
               pc.call_duration AS CallDuration ,
               pc.call_date AS CallDate
        FROM Caller AS cl
            LEFT JOIN PhoneCalls pc ON pc.id = cl.call_id
        WHERE pc.call_type = 'CALL'

        UNION

        SELECT cl.call_id AS CallID,
		       cl.phone_id AS PhoneID,
               pc.call_duration AS CallDuration,
               pc.call_date AS CallDate
        FROM Called AS cl
            LEFT JOIN PhoneCalls pc ON pc.id = cl.call_id
        WHERE pc.call_type = 'CALL'
    ) cl ON cl.PhoneID = ph.id
GROUP BY PersonID;