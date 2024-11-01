WITH latest_transfer AS (
    SELECT 
        player_id,
        to_club_id as latest_transfer_club_id,
        to_club_name as latest_transfer_club_name,
        transfer_date,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY transfer_date DESC) as rn
    FROM {{ source('football_data', 'transfers') }}
),

latest_appearance AS (
    SELECT 
        player_id,
        player_club_id as latest_appearance_club_id,
        DATE as latest_appearance_date,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY DATE DESC) as rn
    FROM {{ source('football_data', 'appearances') }}
),

player_current_status AS (
    SELECT 
        p.player_id,
        p.name as player_name,
        p.current_club_id as recorded_current_club_id,
        p.current_club_name as recorded_current_club_name,
        lt.latest_transfer_club_id,
        lt.latest_transfer_club_name,
        lt.transfer_date as latest_transfer_date,
        la.latest_appearance_club_id,
        la.latest_appearance_date,
        CASE 
            WHEN lt.transfer_date > la.latest_appearance_date THEN lt.latest_transfer_club_id
            WHEN la.latest_appearance_date > lt.transfer_date THEN la.latest_appearance_club_id
            ELSE p.current_club_id
        END as derived_current_club_id,
        CASE 
            WHEN lt.transfer_date > la.latest_appearance_date THEN lt.latest_transfer_club_name
            WHEN la.latest_appearance_date > lt.transfer_date THEN c.name
            ELSE p.current_club_name
        END as derived_current_club_name,
        CASE 
            WHEN lt.transfer_date > la.latest_appearance_date THEN 'Latest Transfer'
            WHEN la.latest_appearance_date > lt.transfer_date THEN 'Latest Appearance'
            ELSE 'Original Record'
        END as club_data_source,
        GREATEST(
            COALESCE(lt.transfer_date, '1900-01-01'),
            COALESCE(la.latest_appearance_date, '1900-01-01')
        ) as last_recorded_date,
        
        CASE 
            WHEN GREATEST(
                COALESCE(lt.transfer_date, '1900-01-01'),
                COALESCE(la.latest_appearance_date, '1900-01-01')
            ) < CURRENT_DATE - INTERVAL '6 months' 
            THEN TRUE 
            ELSE FALSE 
        END as potentially_outdated
    FROM {{ source('football_data', 'players') }} p
    LEFT JOIN latest_transfer lt 
        ON p.player_id = lt.player_id 
        AND lt.rn = 1
    LEFT JOIN latest_appearance la 
        ON p.player_id = la.player_id 
        AND la.rn = 1
    LEFT JOIN {{ source('football_data', 'clubs') }} c 
        ON la.latest_appearance_club_id = c.club_id
)

SELECT 
    *,
    
    CASE 
        WHEN potentially_outdated THEN 
            'Warning: No recent data available. Last record from ' || 
            TO_CHAR(last_recorded_date, 'YYYY-MM-DD')
        ELSE 'Data recent and reliable'
    END as data_quality_note,
    
    CASE 
        WHEN c.domestic_competition_id IS NULL 
        AND derived_current_club_id != recorded_current_club_id 
        THEN TRUE 
        ELSE FALSE 
    END as possible_non_european_transfer
FROM player_current_status pcs
LEFT JOIN {{ source('football_data', 'clubs') }} c 
    ON pcs.derived_current_club_id = c.club_id