with wins as (
	select 
	winner_name as player_name,
	winner_player_id as player_id,
	winner_age_yrs as player_age_yrs,
	tournament_date,
	match_number,
	winner_rank as player_rank,
	1 as match_result,
	1.*safe_divide(winner_serve_games_won, winner_serve_games) as hold_rate,
	1.*safe_divide(winner_return_games_won, winner_return_games) as break_rate
	from {{ ref('stg_matches')}}
	where 1=1 
	and coalesce(winner_serve_games, 1) > 0
	and coalesce(winner_return_games, 1) > 0
),

losses as (
	select 
	loser_name as player_name,
	loser_player_id as player_id,
	loser_age_yrs as player_age_yrs,
	tournament_date,
	match_number, 
	loser_rank as player_rank,
	0 as match_result,
	1.*safe_divide(loser_serve_games_won, loser_serve_games) as hold_rate,
	1.*safe_divide(loser_return_games_won, loser_return_games) as break_rate
	from {{ ref('stg_matches')}}
	where 1=1 
	and coalesce(loser_serve_games, 1) > 0
	and coalesce(loser_return_games, 1) > 0
),

matches as (
	select 
	*,
	row_number() over (partition by player_name order by tournament_date, match_number) as player_match_number,
	row_number() over (partition by player_name, tournament_date order by match_number) as tournament_match_number
	from (
		select * from wins 
		union all 
		select * from losses)
),

results as (
	select 
	player_name,
	player_id,
	player_age_yrs,
	player_rank,
	tournament_date,
	match_result,
	player_match_number,
	tournament_match_number,
	avg(match_result) over (partition by player_name order by tournament_date, tournament_match_number rows between 70 preceding and current row) as rolling_avg_win_rate,
	avg(hold_rate) over (partition by player_name order by tournament_date, tournament_match_number rows between 70 preceding and current row) as rolling_avg_hold_rate,
	avg(break_rate) over (partition by player_name order by tournament_date, tournament_match_number rows between 70 preceding and current row) as rolling_avg_break_rate
	from matches
)

select 
player_name,
player_id,
player_age_yrs,
player_rank,
tournament_date,
player_match_number,
tournament_match_number,
match_result,
rolling_avg_win_rate,
rolling_avg_hold_rate,
rolling_avg_break_rate
from results 