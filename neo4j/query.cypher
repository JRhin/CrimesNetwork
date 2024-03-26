// Query 1: Identify Clusters of Similar Crime Types with Temporal Proximity
// This query aims to find clusters of crimes that not only are of similar types but also occur within close temporal proximity,
// indicating potential patterns or waves of criminal activity without specifying the crime type or date range.

MATCH (c1:Crime)-[:OCCURRED_AT]->(l1:Location), (c2:Crime)-[:OCCURRED_AT]->(l2:Location)
WHERE c1.type = c2.type 
AND c1.date <= c2.date 
AND duration.between(c1.date, c2.date).days <= 30
AND point.distance(point({latitude: l1.latitude, longitude: l1.longitude}), point({latitude: l2.latitude, longitude: l2.longitude})) < 5000
RETURN c1.type AS crime_type, collect(DISTINCT l1.address) + collect(DISTINCT l2.address) AS locations, COUNT(*) AS pattern_strength
ORDER BY pattern_strength DESC
LIMIT 10;


// Query 2: Identify crime hotspots by postcode, aggregating the number of crimes.

MATCH (l:Location)<-[:OCCURRED_AT]-(c:Crime)
RETURN l.postcode, COUNT(c) AS crime_count
ORDER BY crime_count DESC
LIMIT 10;


// Query 3: Analyze the Evolution of Crime Types Over Different Areas
// It calculates the absolute daily change in crime counts from one day to the next instead of growth rates.
// This change measures the volatility without considering the direction of the change (increase or decrease).

MATCH (c:Crime)-[:OCCURRED_AT]->(l:Location)
WITH l.postcode AS area, c.date AS crime_date, COUNT(*) AS crime_count
ORDER BY area, crime_date
WITH area, COLLECT({date: crime_date, count: crime_count}) AS daily_data, SUM(crime_count) as tot_crime_count
UNWIND range(1, SIZE(daily_data) - 1) AS idx
WITH area, daily_data[idx].date AS date, tot_crime_count,
     ABS(daily_data[idx].count - daily_data[idx-1].count) AS daily_change
WITH area, AVG(daily_change) AS avg_daily_change, tot_crime_count
ORDER BY tot_crime_count DESC 
LIMIT 10
RETURN area, ROUND(avg_daily_change, 2);


// Query 4: find the usage frequency for each vehicle type for the "vehicle crimes"

MATCH (v:Vehicle)-[:INVOLVED_IN]->(c:Crime)
WITH v.make AS VehicleBrand, v.model as VehicleModel, COUNT(DISTINCT v.reg) AS VehicleCount, COUNT(DISTINCT c.id) AS CrimeCount
RETURN VehicleBrand, VehicleModel, VehicleCount, CrimeCount
ORDER BY VehicleCount DESC;


// Query 5: for each type of crime type evaluate the standard deviation of the distance between the locations of the related crimes
// in order to understand if crimes are pretty concentrated or spread out in different areas of the city.
// Also report the total number of areas involved in each type of crime.


MATCH (c:Crime) -[:OCCURRED_AT]-> (l:Location)
WITH c.type AS CrimeType,  AVG(l.latitude) AS CentroidLatitude, AVG(l.longitude)  AS CentroidLongitude, apoc.coll.stdev(COLLECT(l.latitude)) AS StdLatitude, apoc.coll.stdev(COLLECT(l.longitude)) AS StdLongitude,  COUNT(DISTINCT l.postcode) AS DifferentArea
RETURN CrimeType, CentroidLatitude, CentroidLongitude, ROUND(111.111 * StdLatitude, 2) AS StdLatitude_km, ROUND(111.111 * COS(StdLatitude)*StdLongitude, 2) AS StdLongitude_km, DifferentArea
ORDER BY DifferentArea DESC;


// Query 6: Identify people that possibly had crime related phone calls.
// This query, for a specific query crime, returns people that have had (as a caller or a called) a lot of calls with people
// that do not belong to their family (order by "No_fam_calls" counter), in the period of 10 days before the crime
// and that live in the same area (post_code) where the crime occurred.

MATCH (crime:Crime)-[:OCCURRED_AT]->(crimeLocation:Location)
WHERE crime.id=47413
WITH crime, crime.date AS crimeDate, crimeLocation.postcode AS crimeArea
MATCH (person:Person)-[:CURRENT_ADDRESS]->(personLocation:Location)
MATCH (person)-[:HAS_PHONE]->(phone:Phone)<-[:CALLER|:CALLED]-(call:PhoneCall)-[:CALLER|:CALLED]->(otherPhone:Phone)<-[:HAS_PHONE]-(otherPerson:Person)
WHERE NOT (person)-[:FAMILY_REL]-(otherPerson) 
  AND call.call_date >= crimeDate - duration('P10D') AND call.call_date < crimeDate
  AND personLocation.postcode = crimeArea
RETURN (person.name +" "+person.surname) AS Person, person.nhs_no AS NHS_Number, phone.phoneNo AS Phone_Number, COUNT(DISTINCT call) AS No_Fam_Calls
ORDER BY No_Fam_Calls DESC


// Query 7: for each officer, return the number of cases to which it has been assigned which last_outcome="Under investigation" 
// as "Num_unresolved", and the most frequent type of crime to which it has been assigned.
// Order the result by "Num_unresolved"

MATCH (c:Crime)-[:INVESTIGATED_BY]->(o:Officer)
WITH c.type AS CrimeType, o.badge_no AS OfficerBadge, COUNT(*) AS CrimeTypeFreq
ORDER BY CrimeTypeFreq DESC
WITH OfficerBadge, COLLECT({CrimeType:CrimeType})[0].CrimeType AS OfficerSpecialization
MATCH (crime:Crime)-[:INVESTIGATED_BY]->(off:Officer)
WHERE off.badge_no = OfficerBadge
    AND crime.last_outcome = "Under investigation"
RETURN  OfficerBadge, OfficerSpecialization, COUNT(DISTINCT crime.id) AS NumUnresolved
ORDER BY NumUnresolved DESC


// Query 8: for a given query date, return the total number of crimes occurred on that day for each area (post_code),
// the total number of people living in each area end the total number of calls made by the inhabitants on that day.

MATCH (c:Crime)-[:OCCURRED_AT]->(l:Location)
WHERE c.date = date("2017-08-06")
WITH l.postcode AS Area, COUNT(DISTINCT c.id) AS TotalCrimes

MATCH (p:Person)-[:CURRENT_ADDRESS]->(l2:Location {postcode: Area})
WITH Area, TotalCrimes, COUNT(p) AS TotalPeople

OPTIONAL MATCH (p)-[:HAS_PHONE]->(ph:Phone)<-[:CALLED|:CALLER]-(cl:PhoneCall)
WHERE cl.call_date = date("2017-08-06") AND cl.call_type = "CALL"
WITH Area, TotalCrimes, TotalPeople, COUNT(DISTINCT cl.id) AS TotalCalls

RETURN Area, TotalCrimes, TotalPeople, TotalCalls
ORDER BY Area


// Query 9: for each Crime Type return the most common outcome, its frequency, the total number of cases and the percentage of the most common outcome over all the total cases

MATCH (c:Crime)
WITH c.type AS CrimeType, c.last_outcome AS LastOutcome, count(*) AS Frequency
ORDER BY Frequency DESC
WITH CrimeType, COLLECT({LastOutcome: LastOutcome, Frequency: Frequency})[0] AS maxFreqPair, SUM(Frequency) AS TotalCases
RETURN  CrimeType, maxFreqPair.LastOutcome AS MostCommonOutcome, maxFreqPair.Frequency AS Frequency, TotalCases, ROUND(100.0 * maxFreqPair.Frequency / TotalCases, 2) AS Percentage
ORDER BY TotalCases DESC