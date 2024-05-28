// NODES

// Locations
LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.nodes.Location.csv' AS row
CREATE (:Location {
  id: toInteger(row.ID),
  latitude: toFloat(row.latitude),
  longitude: toFloat(row.longitude),
  address: row.address,
  postcode: row.postcode
});

// Crimes
LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.nodes.Crime.csv' AS row
CREATE (:Crime {
  id: toInteger(row.ID),
  date: date(apoc.date.format(apoc.date.parse(row.date,'ms', 'dd/mm/yyyy'),'ms','yyyy-mm-dd')),
  type: row.type,
  last_outcome: row.last_outcome,
  note: row.note,
  charge: row.charge
});

// Officer
LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.nodes.Officer.csv' AS row
CREATE (:Officer {
  id: toInteger(row.ID),
  badge_no: row.badge_no,
  rank: row.rank,
  name: row.name,
  surname: row.surname
});

// People
LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.nodes.Person.csv' AS row
CREATE (:Person {
  id: toInteger(row.ID),
  name: row.name,
  surname: row.surname,
  age: toInteger(row.age),
  nhs_no: row.nhs_no
});

// Phone Calls
LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.nodes.PhoneCall.csv' AS row
CREATE (:PhoneCall {
  id: toInteger(row.ID),
  call_duration: toInteger(row.call_duration),
  call_time: time(row.call_time),
  call_date: date(apoc.date.format(apoc.date.parse(row.call_date,'ms', 'dd/mm/yyyy'),'ms','yyyy-mm-dd')),
  call_type: row.call_type
});

// Phones
LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.nodes.Phone.csv' AS row
CREATE (:Phone {
  id: toInteger(row.ID),
  phoneNo: row.phoneNo
});

// Emails
LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.nodes.Email.csv' AS row
CREATE (:Email {
  id: toInteger(row.ID),
  email_address: row.email_address
});

// Vehicles
LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.nodes.Vehicle.csv' AS row
CREATE (:Vehicle {
  id: toInteger(row.ID),
  model: row.model,
  reg: row.reg,
  make: row.make,
  year: toInteger(row.year)
});

// INDEXES

// For 'Location' nodes
CREATE INDEX FOR (l:Location) ON (l.id);

// For 'Crime' nodes
CREATE INDEX FOR (c:Crime) ON (c.id);

// For 'Officer' nodes
CREATE INDEX FOR (o:Officer) ON (o.id);

// For 'Object' nodes
CREATE INDEX FOR (obj:Object) ON (obj.id);

// For 'Person' nodes
CREATE INDEX FOR (p:Person) ON (p.id);

// For 'PhoneCall' nodes
CREATE INDEX FOR (pc:PhoneCall) ON (pc.id);

// For 'Phone' nodes
CREATE INDEX FOR (ph:Phone) ON (ph.id);

// For 'Email' nodes
CREATE INDEX FOR (e:Email) ON (e.id);

// For 'Vehicle' nodes
CREATE INDEX FOR (v:Vehicle) ON (v.id);


// RELATIONS

CALL apoc.periodic.iterate(
"LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.relationships.CURRENT_ADDRESS.csv' AS row RETURN row",
"MATCH (person:Person {id: toInteger(row.`:START_ID`)}), (location:Location {id: toInteger(row.`:END_ID`)})
CREATE (person)-[:CURRENT_ADDRESS]->(location)",
{batchSize:1000, parallel:true}
);

CALL apoc.periodic.iterate(
"LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.relationships.INVESTIGATED_BY.csv' AS row RETURN row",
"MATCH (crime:Crime {id: toInteger(row.`:START_ID`)}), (officer:Officer {id: toInteger(row.`:END_ID`)})
CREATE (crime)-[:INVESTIGATED_BY]->(officer)",
{batchSize:1000, parallel:true}
);

CALL apoc.periodic.iterate(
"LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.relationships.HAS_EMAIL.csv' AS row RETURN row",
"MATCH (person:Person {id: toInteger(row.`:START_ID`)}), (email:Email {id: toInteger(row.`:END_ID`)})
CREATE (person)-[:HAS_EMAIL]->(email)",
{batchSize:1000, parallel:true}
);

CALL apoc.periodic.iterate(
"LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.relationships.INVOLVED_IN.csv' AS row RETURN row",
"MATCH (v:Vehicle {id: toInteger(row.`:START_ID`)}), (c:Crime {id: toInteger(row.`:END_ID`)})
CREATE (v)-[:INVOLVED_IN]->(c)",
{batchSize:1000, parallel:true}
);

CALL apoc.periodic.iterate(
"LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.relationships.OCCURRED_AT.csv' AS row RETURN row",
"MATCH (c:Crime {id: toInteger(row.`:START_ID`)}), (l:Location {id: toInteger(row.`:END_ID`)})
CREATE (c)-[:OCCURRED_AT]->(l)",
{batchSize:1000, parallel:true}
);

CALL apoc.periodic.iterate(
"LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.relationships.HAS_PHONE.csv' AS row RETURN row",
"MATCH (p:Person {id: toInteger(row.`:START_ID`)}), (ph:Phone {id: toInteger(row.`:END_ID`)})
CREATE (p)-[:HAS_PHONE]->(ph)",
{batchSize:1000, parallel:true}
);

CALL apoc.periodic.iterate(
"LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.relationships.FAMILY_REL.csv' AS row RETURN row",
"MATCH (p1:Person {id: toInteger(row.`:START_ID`)}), (p2:Person {id: toInteger(row.`:END_ID`)})
CREATE (p1)-[:FAMILY_REL {rel_type: row.rel_type}]-(p2)",
{batchSize:1000, parallel:true}
);

CALL apoc.periodic.iterate(
"LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.relationships.KNOWS.csv' AS row RETURN row",
"MATCH (p1:Person {id: toInteger(row.`:START_ID`)}), (p2:Person {id: toInteger(row.`:END_ID`)})
CREATE (p1)-[:KNOWS]->(p2)",
{batchSize:1000, parallel:true}
);

CALL apoc.periodic.iterate(
"LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.relationships.CALLER.csv' AS row RETURN row",
"MATCH (pc:PhoneCall {id: toInteger(row.`:START_ID`)}), (ph:Phone {id: toInteger(row.`:END_ID`)})
CREATE (pc)-[:CALLER]->(ph)",
{batchSize:1000, parallel:true}
);

CALL apoc.periodic.iterate(
"LOAD CSV WITH HEADERS FROM 'file:///crime-investigation.relationships.CALLED.csv' AS row RETURN row",
"MATCH (pc:PhoneCall {id: toInteger(row.`:START_ID`)}), (ph:Phone {id: toInteger(row.`:END_ID`)})
CREATE (pc)-[:CALLED]->(ph)",
{batchSize:1000, parallel:true}
);