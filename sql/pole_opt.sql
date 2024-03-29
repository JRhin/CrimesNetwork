CREATE TABLE Locations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL,
    address VARCHAR(255) NOT NULL,
    postcode VARCHAR(20)
);

CREATE TABLE Crimes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL CHECK (date <= CURRENT_DATE),
    type VARCHAR(50) NOT NULL,
    last_outcome VARCHAR(255),
    note TEXT,
    charge VARCHAR(255),
    location_id INT UNSIGNED,
    FOREIGN KEY (location_id) REFERENCES Locations(id)
);


CREATE TABLE Officer (
    id INT AUTO_INCREMENT PRIMARY KEY,
    badge_no VARCHAR(20) NOT NULL UNIQUE,
    rank VARCHAR(30) NOT NULL,
    name VARCHAR(100) NOT NULL,
    surname VARCHAR(100) NOT NULL
);


CREATE TABLE People (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    surname VARCHAR(100),
    age TINYINT UNSIGNED CHECK (age >= 0 AND age <= 120),
    nhs_no VARCHAR(20) UNIQUE
);


CREATE TABLE PhoneCalls (
    id INT AUTO_INCREMENT PRIMARY KEY,
    call_date DATE,
    call_duration SMALLINT UNSIGNED,
    call_time TIME,
    from_phone_id INT UNSIGNED,
    to_phone_id INT UNSIGNED,
    FOREIGN KEY (from_phone_id) REFERENCES Phones(id),
    FOREIGN KEY (to_phone_id) REFERENCES Phones(id),
    FOREIGN KEY (crime_id) REFERENCES Crimes(id)
);


CREATE TABLE Phones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    owner_id INT UNSIGNED,
    phoneNo VARCHAR(20),
    FOREIGN KEY (owner_id) REFERENCES People(id)
);


CREATE TABLE Vehicles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    make VARCHAR(50),
    model VARCHAR(50),
    year YEAR,
    color VARCHAR(50),
    license_plate VARCHAR(50),
    owner_id INT,
    FOREIGN KEY (owner_id) REFERENCES People(id)
);