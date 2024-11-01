WITH player_stats AS (
    SELECT
        a.PLAYER_ID,
        p.NAME as player_name,
        p.POSITION,
        p.SUB_POSITION,
        EXTRACT(YEAR FROM g.DATE)::int - EXTRACT(YEAR FROM p.DATE_OF_BIRTH)::int - 
        (CASE 
            WHEN EXTRACT(MONTH FROM g.DATE) < EXTRACT(MONTH FROM p.DATE_OF_BIRTH) OR 
                (EXTRACT(MONTH FROM g.DATE) = EXTRACT(MONTH FROM p.DATE_OF_BIRTH) AND 
                 EXTRACT(DAY FROM g.DATE) < EXTRACT(DAY FROM p.DATE_OF_BIRTH))
            THEN 1
            ELSE 0
        END) as player_age,
        g.SEASON,
        c_historical.NAME as playing_club_name,  -- renamed from historical_club_name
        comp.NAME as competition_name,
        COUNT(DISTINCT a.GAME_ID) as games_played,
        SUM(a.GOALS) as total_goals,
        SUM(a.ASSISTS) as total_assists,
        SUM(a.MINUTES_PLAYED) as total_minutes,
        SUM(a.YELLOW_CARDS) as total_yellow_cards,
        SUM(a.RED_CARDS) as total_red_cards
    FROM {{ source('football_data', 'appearances') }} a
    JOIN {{ source('football_data', 'games') }} g
        ON a.GAME_ID = g.GAME_ID
    JOIN {{ source('football_data', 'players') }} p
        ON a.PLAYER_ID = p.PLAYER_ID
    LEFT JOIN {{ source('football_data', 'clubs') }} c_historical
        ON a.PLAYER_CLUB_ID = c_historical.CLUB_ID
    JOIN {{ source('football_data', 'competitions') }} comp
        ON a.COMPETITION_ID = comp.COMPETITION_ID
    GROUP BY 1,2,3,4,5,6,7,8
)
SELECT 
    ps.*,
    pcc.derived_current_club_name as current_club,
    pcc.data_quality_note,
    
    -- Calculated metrics
    ROUND(ps.total_goals::float / NULLIF(ps.games_played, 0), 2) as goals_per_game,
    ROUND(ps.total_assists::float / NULLIF(ps.games_played, 0), 2) as assists_per_game,
    ROUND(ps.total_minutes::float / NULLIF(ps.games_played, 0), 0) as avg_minutes_per_game,
    ROUND(ps.total_yellow_cards::float / NULLIF(ps.games_played, 0), 2) as yellow_cards_per_game,
    ROUND(ps.total_red_cards::float / NULLIF(ps.games_played, 0), 2) as red_cards_per_game,
    ROUND((ps.total_goals + ps.total_assists)::float / NULLIF(ps.games_played, 0), 2) as goal_contributions_per_game
FROM player_stats ps
LEFT JOIN {{ ref('player_current_club') }} pcc
    ON ps.PLAYER_ID = pcc.player_id