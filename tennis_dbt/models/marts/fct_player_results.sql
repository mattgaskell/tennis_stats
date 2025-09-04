with wins as (
	select 
	winner_name as player_name,
	winner_age_yrs as player_age_yrs,
	tournament_date,
	row_number() over (partition by winner_name order by tournament_date, match_number) as player_match_number,
	row_number() over (partition by winner_name, tournament_date order by match_number) as tournament_match_number,
	1 as match_result,
	1.*winner_serve_games_won/winner_serve_games as hold_rate,
	1.*winner_return_games_won/winner_return_games as break_rate
	from {{ ref('stg_matches')}}
),

losses as (
	select 
	loser_name as player_name,
	loser_age_yrs as player_age_yrs,
	tournament_date,
	row_number() over (partition by loser_name order by tournament_date, match_number) as player_match_number,
	row_number() over (partition by loser_name, tournament_date order by match_number) as tournament_match_number,
	0 as match_result,
	1.*loser_serve_games_won/loser_serve_games as hold_rate,
	1.*loser_return_games_won/loser_return_games as break_rate
	from {{ ref('stg_matches')}}
),

matches as (
	select * from wins 
	union all 
	select * from losses
),

results as (
	select 
	player_name,
	player_age_yrs,
	tournament_date,
	player_match_number,
	tournament_match_number,
	match_result,
	avg(match_result) over (partition by player_name order by tournament_date, tournament_match_number rows between 50 preceding and current row) as rolling_avg_win_rate,
	avg(hold_rate) over (partition by player_name order by tournament_date, tournament_match_number rows between 50 preceding and current row) as rolling_avg_hold_rate,
	avg(break_rate) over (partition by player_name order by tournament_date, tournament_match_number rows between 50 preceding and current row) as rolling_avg_break_rate
	from matches
	order by 1,2,3
)

select 
player_name,
player_age_yrs,
tournament_date,
player_match_number,
tournament_match_number,
match_result,
rolling_avg_win_rate,
rolling_avg_hold_rate,
rolling_avg_break_rate
from results 
where player_match_number >= 15