// Query 1: Find police officers linked to over 30 unique crime investigations, and list these officers' surnames,
// badge numbers, and ranks, sorting the results alphabetically by surname.
// COMPLETED AFTER 62 ms.

MATCH (o:Officer)<-[:INVESTIGATED_BY]-(c:Crime)
WITH o, COUNT(DISTINCT c.id) AS TotCrimes
WHERE TotCrimes > 30
RETURN o.surname AS Surname, o.badge_no AS BadgeNo, o.rank AS Rank
ORDER BY o.badge_no


// Query 2: Identify crime hotspots by postcode, aggregating the number of crimes.
// COMPLETED AFTER 178 ms.

MATCH (l:Location)<-[:OCCURRED_AT]-(c:Crime)
RETURN l.postcode, COUNT(c) AS crime_count
ORDER BY crime_count DESC
LIMIT 10;


// Query 3: Analyze the Evolution of Crime Types Over Different Areas
// It calculates the absolute daily change in crime counts from one day to the next instead of growth rates.
// This change measures the volatility without considering the direction of the change (increase or decrease).
// COMPLETED AFTER 376 ms.

MATCH (c:Crime)-[:OCCURRED_AT]->(l:Location)
WITH l.postcode AS Area, c.date AS CrimeDate, COUNT(DISTINCT c.id) AS CrimeCount
ORDER BY Area, CrimeDate
WITH Area, COLLECT({date: CrimeDate, count: CrimeCount}) AS DailyData
UNWIND range(1, SIZE(DailyData) - 1) AS idx
WITH Area, ABS(DailyData[idx].count - DailyData[idx-1].count) AS DailyChange
WITH Area, ROUND(AVG(DailyChange), 2) AS AvgDailyChange
ORDER BY AvgDailyChange DESC
LIMIT 10
RETURN Area, AvgDailyChange;


// Query 4: Find the usage frequency for each vehicle type for the "vehicle crimes".
// COMPLETED AFTER 23 ms.

MATCH (v:Vehicle)-[:INVOLVED_IN]->(c:Crime)
WITH v.make AS VehicleBrand, v.model as VehicleModel, COUNT(DISTINCT v.reg) AS VehicleCount, COUNT(DISTINCT c.id) AS CrimeCount
RETURN VehicleBrand, VehicleModel, VehicleCount, CrimeCount
ORDER BY VehicleCount DESC;


// Query 5: For each type of crime type evaluate the standard deviation of the distance between the locations of the related crimes
// in order to understand if crimes are pretty concentrated or spread out in different areas of the city.
// Also report the total number of areas involved in each type of crime.
// COMPLETED AFTER 142 ms.

MATCH (c:Crime) -[:OCCURRED_AT]-> (l:Location)
WITH c.type AS CrimeType,  AVG(l.latitude) AS CentroidLatitude, AVG(l.longitude)  AS CentroidLongitude, apoc.coll.stdev(COLLECT(l.latitude)) AS StdLatitude, apoc.coll.stdev(COLLECT(l.longitude)) AS StdLongitude,  COUNT(DISTINCT l.postcode) AS DifferentArea
RETURN CrimeType, CentroidLatitude, CentroidLongitude, ROUND(111.111 * StdLatitude, 2) AS StdLatitude_km, ROUND(111.111 * COS(StdLatitude)*StdLongitude, 2) AS StdLongitude_km, DifferentArea
ORDER BY DifferentArea DESC;


// Query 6: Identify people that possibly had crime related phone calls.
// This query, for a specific query crime, returns people that have had (as a caller or a called) a lot of calls with people
// that do not belong to their family (order by "No_fam_calls" counter), in the period of 10 days before the crime
// and that live in the same area (post_code) where the crime occurred.
// COMPLETED AFTER 56 ms.

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


// Query 7: For each officer, return the number of cases to which it has been assigned which last_outcome="Under investigation" 
// as "Num_unresolved", and the most frequent type of crime to which it has been assigned.
// Order the result by "Num_unresolved"
// COMPLETED AFTER 563 ms.

MATCH (c:Crime)-[:INVESTIGATED_BY]->(o:Officer)
WITH c.type AS CrimeType, o.badge_no AS OfficerBadge, COUNT(*) AS CrimeTypeFreq
ORDER BY CrimeTypeFreq DESC
WITH OfficerBadge, COLLECT({CrimeType:CrimeType})[0].CrimeType AS OfficerSpecialization
MATCH (crime:Crime)-[:INVESTIGATED_BY]->(off:Officer)
WHERE off.badge_no = OfficerBadge
    AND crime.last_outcome = "Under investigation"
RETURN  OfficerBadge, OfficerSpecialization, COUNT(DISTINCT crime.id) AS NumUnresolved
ORDER BY NumUnresolved DESC


// Query 8: For a given query date, return the total number of crimes occurred on that day for each area (post_code),
// the total number of people living in each area end the total number of calls made by the inhabitants on that day.
// COMPLETED AFTER 24 ms.

MATCH (c:Crime)-[:OCCURRED_AT]->(l:Location)<-[:CURRENT_ADDRESS]-(p:Person)-[:HAS_PHONE]->(ph:Phone)<-[:CALLED|:CALLER]-(cl:PhoneCall)
WITH l.postcode AS Area, COUNT(DISTINCT c.id) AS TotalCrimes, COUNT(DISTINCT p) AS TotalPeople, COUNT(DISTINCT cl.id) AS TotalTraffic
RETURN Area, TotalCrimes, TotalPeople, TotalTraffic
ORDER BY TotalCrimes DESC, TotalTraffic DESC, TotalPeople DESC, Area DESC


// Query 9: For each Crime Type return the most common outcome, its frequency,
// the total number of cases and the percentage of the most common outcome over all the total cases.
// COMPLETED AFTER 76 ms.

MATCH (c:Crime)
WITH c.type AS CrimeType, c.last_outcome AS LastOutcome, count(*) AS Frequency
ORDER BY Frequency DESC
WITH CrimeType, COLLECT({LastOutcome: LastOutcome, Frequency: Frequency})[0] AS maxFreqPair, SUM(Frequency) AS TotalCases
RETURN  CrimeType, maxFreqPair.LastOutcome AS MostCommonOutcome, maxFreqPair.Frequency AS Frequency, TotalCases, ROUND(100.0 * maxFreqPair.Frequency / TotalCases, 2) AS Percentage
ORDER BY TotalCases DESC


// Query 10: This query aggregates phone communication data for individuals, calculating metrics like the number of distinct phones per person,
// total and average call durations, total different calls, and the number of unique call days.
// COMPLETED AFTER 7 ms.

MATCH (p:Person)-[:HAS_PHONE]->(ph:Phone)<-[:CALLER|:CALLED]-(cl:PhoneCall {call_type: 'CALL'})
WITH p.id AS PersonID,
     COUNT(DISTINCT ph) AS NumDistinctPhones,
     SUM(cl.call_duration) AS TotalCallTime,
     AVG(cl.call_duration) AS AvgCallDuration,
     COUNT(DISTINCT cl) AS TotCalls,
     COUNT(DISTINCT cl.call_date) AS DistinctCallDate
RETURN PersonID,
       NumDistinctPhones,
       TotalCallTime,
       ROUND(AvgCallDuration, 2) AS AvgCallDuration,
       TotCalls,
       DistinctCallDate
ORDER BY PersonID
