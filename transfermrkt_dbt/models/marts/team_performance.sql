WITH team_season_games AS (
    SELECT
        g.HOME_CLUB_ID as club_id,
        c.NAME as club_name,
        comp.NAME as competition_name,
        comp_domestic.COUNTRY_NAME as club_nationality,
        g.SEASON,
        COUNT(DISTINCT g.GAME_ID) as total_games,
        SUM(CASE WHEN g.HOME_CLUB_GOALS > g.AWAY_CLUB_GOALS THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN g.HOME_CLUB_GOALS = g.AWAY_CLUB_GOALS THEN 1 ELSE 0 END) as draws,
        SUM(CASE WHEN g.HOME_CLUB_GOALS < g.AWAY_CLUB_GOALS THEN 1 ELSE 0 END) as losses,
        SUM(g.HOME_CLUB_GOALS) as goals_for,
        SUM(g.AWAY_CLUB_GOALS) as goals_against,
        AVG(g.ATTENDANCE) as avg_attendance,
        'Home' as game_type
    FROM {{ source('football_data', 'games') }} g
    JOIN {{ source('football_data', 'clubs') }} c
        ON g.HOME_CLUB_ID = c.CLUB_ID
    JOIN {{ source('football_data', 'competitions') }} comp
        ON g.COMPETITION_ID = comp.COMPETITION_ID
    LEFT JOIN {{ source('football_data', 'competitions') }} comp_domestic
        ON c.DOMESTIC_COMPETITION_ID = comp_domestic.COMPETITION_ID
    GROUP BY 1,2,3,4,5
    
    UNION ALL
    
    SELECT
        g.AWAY_CLUB_ID as club_id,
        c.NAME as club_name,
        comp.NAME as competition_name,
        comp_domestic.COUNTRY_NAME as club_nationality,  -- Added this
        g.SEASON,
        COUNT(DISTINCT g.GAME_ID) as total_games,
        SUM(CASE WHEN g.AWAY_CLUB_GOALS > g.HOME_CLUB_GOALS THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN g.AWAY_CLUB_GOALS = g.HOME_CLUB_GOALS THEN 1 ELSE 0 END) as draws,
        SUM(CASE WHEN g.AWAY_CLUB_GOALS < g.HOME_CLUB_GOALS THEN 1 ELSE 0 END) as losses,
        SUM(g.AWAY_CLUB_GOALS) as goals_for,
        SUM(g.HOME_CLUB_GOALS) as goals_against,
        AVG(g.ATTENDANCE) as avg_attendance,
        'Away' as game_type
    FROM {{ source('football_data', 'games') }} g
    JOIN {{ source('football_data', 'clubs') }} c
        ON g.AWAY_CLUB_ID = c.CLUB_ID
    JOIN {{ source('football_data', 'competitions') }} comp
        ON g.COMPETITION_ID = comp.COMPETITION_ID
    LEFT JOIN {{ source('football_data', 'competitions') }} comp_domestic
        ON c.DOMESTIC_COMPETITION_ID = comp_domestic.COMPETITION_ID
    GROUP BY 1,2,3,4,5
),
seasonal_stats AS (
    SELECT
        club_id,
        club_name,
        competition_name,
        club_nationality,
        SEASON,
        SUM(total_games) as total_games,
        SUM(wins) as total_wins,
        SUM(draws) as total_draws,
        SUM(losses) as total_losses,
        SUM(goals_for) as total_goals_for,
        SUM(goals_against) as total_goals_against,
        AVG(avg_attendance) as avg_attendance
    FROM team_season_games
    GROUP BY 1,2,3,4,5
)
SELECT
    club_id,
    club_name,
    competition_name,
    club_nationality,
    SEASON,
    total_games,
    total_wins,
    total_draws,
    total_losses,
    total_goals_for,
    total_goals_against,
    total_goals_for - total_goals_against as goal_difference,
    ROUND(avg_attendance, 0) as avg_attendance,
    ROUND(total_wins::float / NULLIF(total_games, 0) * 100, 2) as win_percentage,
    ROUND(total_draws::float / NULLIF(total_games, 0) * 100, 2) as draw_percentage,
    ROUND(total_losses::float / NULLIF(total_games, 0) * 100, 2) as loss_percentage,
    ROUND(total_goals_for::float / NULLIF(total_games, 0), 2) as avg_goals_scored_per_game,
    ROUND(total_goals_against::float / NULLIF(total_games, 0), 2) as avg_goals_conceded_per_game,
    total_wins * 3 + total_draws as total_points,
    ROUND(
        (total_wins * 3 + total_draws)::float / NULLIF(total_games, 0),
    2) as points_per_game
FROM seasonal_stats
ORDER BY club_name, SEASON DESC