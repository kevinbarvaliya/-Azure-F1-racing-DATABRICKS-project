-- Databricks notebook source
select team_name,
       count(1) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) avg_points,
       rank() over(order by avg(calculated_points) desc) as team_rank
  from f1_presentation.calculated_race_results
group by team_name
having total_races >= 100
order by avg_points desc;

-- COMMAND ----------

create or replace view v_dominant_team as(
    select team_name,
           count(1) as total_races,
           sum(calculated_points) as total_points,
           avg(calculated_points) avg_points,
           rank() over(order by avg(calculated_points) desc) as team_rank
      from f1_presentation.calculated_race_results
    group by team_name
    having total_races >= 100
    order by avg_points desc);

-- COMMAND ----------

select * from v_dominant_team 


-- COMMAND ----------

select race_year,
       team_name,
       count(1) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) avg_points,
       rank() over(order by avg(calculated_points) desc) as team_rank
from f1_presentation.calculated_race_results
where team_name in (select team_name from v_dominant_team where team_rank <= 10)
group by race_year, team_name
order by race_year, avg_points desc;

-- COMMAND ----------

select race_year,
       team_name,
       count(1) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) avg_points,
       rank() over(order by avg(calculated_points) desc) as team_rank
  from f1_presentation.calculated_race_results
where team_name in (select team_name from v_dominant_team where team_rank <= 10)
group by race_year, team_name
order by race_year, avg_points desc;

-- COMMAND ----------

select race_year,
       team_name,
       count(1) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) avg_points,
       rank() over(order by avg(calculated_points) desc) as team_rank
  from f1_presentation.calculated_race_results
where team_name in (select team_name from v_dominant_team where team_rank <= 10)
group by race_year, team_name
order by race_year, avg_points desc;

-- COMMAND ----------


