-- Create partitioned and clustered table
DROP TABLE IF EXISTS dtc-de-404803.project_data.covid_partitioned_clustered;
CREATE OR REPLACE TABLE dtc-de-404803.project_data.covid_partitioned_clustered
PARTITION BY DATE_TRUNC(ObservationDate, MONTH)
CLUSTER BY Province_State
AS SELECT * FROM dtc-de-404803.project_data.covid;

-- Calculate statistics for each province
select Province_State, max(Confirmed) as FinalConfirmed, max(Deaths) as FinalDeaths, max(Recovered) as FinalRecovered, IF(max(Confirmed)-max(Deaths)-max(Recovered)>=0,max(Confirmed)-max(Deaths)-max(Recovered),0) as UnderRecovery
from dtc-de-404803.project_data.covid_partitioned_clustered
where DATE_TRUNC(ObservationDate,MONTH) = '2020-12-01'
group by Province_State
order by Province_State;

select Province_State, max(Confirmed) as FinalConfirmed, max(Deaths) as FinalDeaths, max(Recovered) as FinalRecovered, IF(max(Confirmed)-max(Deaths)-max(Recovered)>=0,max(Confirmed)-max(Deaths)-max(Recovered),0) as UnderRecovery
from dtc-de-404803.project_data.covid
where DATE_TRUNC(ObservationDate,MONTH) = '2020-12-01'
group by Province_State
order by Province_State;

-- Create reports
create or replace table dtc-de-404803.project_data.covid_report_2020 as
select Province_State, max(Confirmed) as FinalConfirmed, max(Deaths) as FinalDeaths, max(Recovered) as FinalRecovered, IF(max(Confirmed)-max(Deaths)-max(Recovered)>=0,max(Confirmed)-max(Deaths)-max(Recovered),0) as UnderRecovery
from dtc-de-404803.project_data.covid_partitioned_clustered
where DATE_TRUNC(ObservationDate,MONTH) = '2020-12-01'
group by Province_State
order by Province_State;

create or replace table dtc-de-404803.project_data.covid_report_2021 as
select Province_State, max(Confirmed) as FinalConfirmed, max(Deaths) as FinalDeaths, max(Recovered) as FinalRecovered, IF(max(Confirmed)-max(Deaths)-max(Recovered)>=0,max(Confirmed)-max(Deaths)-max(Recovered),0) as UnderRecovery
from dtc-de-404803.project_data.covid_partitioned_clustered
where DATE_TRUNC(ObservationDate,MONTH) = '2021-05-01'
group by Province_State
order by Province_State;

-- Create Table for ML
select Province_State, max(Confirmed) as FinalConfirmed, max(Deaths) as FinalDeaths, max(Recovered) as FinalRecovered, max(Lat), max(Long)
from dtc-de-404803.project_data.covid_partitioned_clustered
where DATE_TRUNC(ObservationDate,MONTH) = '2021-05-01' and Province_State!='Unknown'
group by Province_State
order by Province_State;