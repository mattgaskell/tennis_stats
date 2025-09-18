with wins as (
	select 
	winner_player_id,
	winner_name as player_name,
	extract(year from tournament_date) as year,
	winner_height_cm as player_height_cm,
	min(winner_age_yrs) as player_age_yrs_start_yr,
	min(winner_rank) as player_best_rank,
	count(*) as wins,
	sum(case when surface = 'Clay' then 1 end) as clay_wins,
	sum(case when surface = 'Grass' then 1 end) as grass_wins,
	sum(case when surface = 'Hard' then 1 end) as hard_wins,
	sum(pts) as pts,
	sum(winner_pts_won) as winner_pts_won,
	sum(games) as games,
	sum(winner_games_won) as winner_games_won,
	sum(winner_serve_pts) as winner_serve_pts,
	sum(winner_serve_pts_won) as winner_serve_pts_won,
	sum(winner_return_pts) as winner_return_pts,
	sum(winner_return_pts_won) as winner_return_pts_won,
	sum(winner_serve_games) as winner_serve_games,
	sum(winner_serve_games_won) as winner_serve_games_won,
	sum(winner_return_games) as winner_return_games,
	sum(winner_return_games_won) as winner_return_games_won,
	sum(winner_aces) as winner_aces,
	sum(winner_dfs) as winner_dfs,
	sum(winner_1st_serves_in) as winner_1st_serves_in
	from {{ ref('stg_matches')}}
	group by 1,2,3,4
),

losses as (
	select 
	loser_player_id,
	loser_name as player_name,
	extract(year from tournament_date) as year,
	loser_height_cm as player_height_cm,
	count(*) as losses,
	sum(case when surface = 'Clay' then 1 end) as clay_losses,
	sum(case when surface = 'Grass' then 1 end) as grass_losses,
	sum(case when surface = 'Hard' then 1 end) as hard_losses,
	sum(pts) as pts,
	sum(loser_pts_won) as loser_pts_won,
	sum(games) as games,
	sum(loser_games_won) as loser_games_won,
	sum(loser_serve_pts) as loser_serve_pts,
	sum(loser_serve_pts_won) as loser_serve_pts_won,
	sum(loser_return_pts) as loser_return_pts,
	sum(loser_return_pts_won) as loser_return_pts_won,
	sum(loser_serve_games) as loser_serve_games,
	sum(loser_serve_games_won) as loser_serve_games_won,
	sum(loser_return_games) as loser_return_games,
	sum(loser_return_games_won) as loser_return_games_won,
	sum(loser_aces) as loser_aces,
	sum(loser_dfs) as loser_dfs,
	sum(loser_1st_serves_in) as loser_1st_serves_in
	from {{ ref('stg_matches')}}
	group by 1,2,3,4
)


select 
w.winner_player_id as player_id,
w.year,
w.player_name,
w.player_height_cm,
player_age_yrs_start_yr,
player_best_rank,
w.wins + l.losses as matches,
w.wins,
l.losses,
clay_wins,
clay_losses,
hard_wins,
hard_losses,
grass_wins,
grass_losses,
100.*safe_divide(wins, wins + losses) as pc_win_rate,
100.*safe_divide(clay_wins, clay_wins + clay_losses) as pc_clay_win_rate,
100.*safe_divide(hard_wins, hard_wins + hard_losses) as pc_hard_win_rate,
100.*safe_divide(grass_wins, grass_wins + grass_losses) as pc_grass_win_rate,
100.*safe_divide(winner_serve_pts_won + loser_serve_pts_won, winner_serve_pts + loser_serve_pts) as pc_serve_pts_won,
100.*safe_divide(winner_return_pts_won + loser_return_pts_won, winner_return_pts + loser_return_pts) as pc_return_pts_won,
100.*safe_divide(winner_pts_won + loser_pts_won, w.pts + l.pts) as pc_pts_won,
100.*safe_divide(winner_serve_games_won + loser_serve_games_won, winner_serve_games + loser_serve_games) as pc_serve_games_won,
100.*safe_divide(winner_return_games_won + loser_return_games_won, winner_return_games + loser_return_games) as pc_return_games_won,
100.*safe_divide(winner_games_won + loser_games_won, w.games + l.games) as pc_games_won,
100.*safe_divide(winner_1st_serves_in + loser_1st_serves_in, winner_serve_pts + loser_serve_pts) as pc_1st_serve_in,
100.*safe_divide(winner_dfs + loser_dfs, winner_serve_pts + loser_serve_pts) as pc_dfs
from wins w inner join losses l 
on w.winner_player_id = l.loser_player_id
and w.player_name = l.player_name
and w.year = l.year
and w.player_height_cm = l.player_height_cm
order by 1,2