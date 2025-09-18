select 
winner_name as player_name,
tournament_date,
winner_age_yrs,
tournament_name,
tournament_level,
count(*) over (partition by winner_name order by tournament_date) as cum_tournaments_won,
count(case when tournament_level = 'G' then 1 end) over (partition by winner_name order by tournament_date) as cum_slams_won,
count(case when tournament_level = 'M' then 1 end) over (partition by winner_name order by tournament_date) as cum_masters_won
from {{ ref('stg_matches')}}
where 1=1 
and round = 'F'
and tournament_level in ('G', 'M', 'A', 'F', 'D')
order by 1,2