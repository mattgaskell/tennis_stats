with wins as (
	select 
	winner_name as player_name,
	extract(year from tournament_date) as year,
	count(*) as wins,
	sum(case when surface = 'Clay' then 1 end) as clay_wins,
	sum(case when surface = 'Grass' then 1 end) as grass_wins,
	sum(case when surface = 'Hard' then 1 end) as hard_wins,
	sum(winner_serve_pts_won) as winner_serve_pts_won,
	sum(winner_serve_pts) as winner_serve_pts
	from {{ ref('stg_matches')}}
	group by 1,2
),

losses as (
	select 
	loser_name as player_name,
	extract(year from tournament_date) as year,
	count(*) as losses,
	sum(case when surface = 'Clay' then 1 end) as clay_losses,
	sum(case when surface = 'Grass' then 1 end) as grass_losses,
	sum(case when surface = 'Hard' then 1 end) as hard_losses,
	sum(loser_serve_pts_won) as loser_serve_pts_won,
	sum(loser_serve_pts) as loser_serve_pts
	from {{ ref('stg_matches')}}
	group by 1,2
)


select 
w.player_name,
w.year,
wins + losses as matches,
wins,
losses,
clay_wins,
clay_losses,
hard_wins,
hard_losses,
grass_wins,
grass_losses,
100.*wins/(wins + losses) as pc_win_rate,
100.*clay_wins/(clay_wins + clay_losses) as pc_clay_win_rate,
100.*hard_wins/(hard_wins + hard_losses) as pc_hard_win_rate,
100.*grass_wins/(grass_wins + grass_losses) as pc_grass_win_rate,
100.*(winner_serve_pts_won + loser_serve_pts_won)/(winner_serve_pts + loser_serve_pts) as pc_serve_pts_won
from wins w inner join losses l 
on w.player_name = l.player_name
and w.year = l.year
order by 1,2