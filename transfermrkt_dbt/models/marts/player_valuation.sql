WITH latest_valuation AS (
    SELECT 
        PLAYER_ID,
        MARKET_VALUE_IN_EUR,
        DATE as last_valuation_date,
        ROW_NUMBER() OVER (PARTITION BY PLAYER_ID ORDER BY DATE DESC) as rn
    FROM {{ source('football_data', 'player_valuations') }}
),
latest_transfer AS (
    SELECT 
        PLAYER_ID,
        TRANSFER_FEE,
        TRANSFER_DATE,
        ROW_NUMBER() OVER (PARTITION BY PLAYER_ID ORDER BY TRANSFER_DATE DESC) as rn
    FROM {{ source('football_data', 'transfers') }}
)
SELECT 
    p.PLAYER_ID,
    p.NAME as player_name,
    p.POSITION,
    p.SUB_POSITION,
    EXTRACT(YEAR FROM CURRENT_DATE)::int - EXTRACT(YEAR FROM p.DATE_OF_BIRTH)::int - 
    (CASE 
        WHEN EXTRACT(MONTH FROM CURRENT_DATE) < EXTRACT(MONTH FROM p.DATE_OF_BIRTH) OR 
            (EXTRACT(MONTH FROM CURRENT_DATE) = EXTRACT(MONTH FROM p.DATE_OF_BIRTH) AND 
             EXTRACT(DAY FROM CURRENT_DATE) < EXTRACT(DAY FROM p.DATE_OF_BIRTH))
        THEN 1
        ELSE 0
    END) as current_age,
    pcc.derived_current_club_name as current_club,
    -- Added league and country information
    comp.NAME as current_league,
    comp.COUNTRY_NAME as league_country,
    pcc.club_data_source,
    pcc.potentially_outdated,
    pcc.data_quality_note,
    p.CONTRACT_EXPIRATION_DATE,
    -- Market value info
    lv.MARKET_VALUE_IN_EUR as current_market_value,
    lv.last_valuation_date,
    p.HIGHEST_MARKET_VALUE_IN_EUR as historical_highest_value,
    -- Latest transfer info
    lt.TRANSFER_FEE as last_transfer_fee,
    lt.TRANSFER_DATE as last_transfer_date
FROM {{ source('football_data', 'players') }} p
LEFT JOIN latest_valuation lv
    ON p.PLAYER_ID = lv.PLAYER_ID 
    AND lv.rn = 1
LEFT JOIN latest_transfer lt
    ON p.PLAYER_ID = lt.PLAYER_ID
    AND lt.rn = 1
LEFT JOIN {{ ref('player_current_club') }} pcc
    ON p.PLAYER_ID = pcc.player_id
-- Added these joins to get league and country
LEFT JOIN {{ source('football_data', 'clubs') }} c
    ON pcc.derived_current_club_id = c.CLUB_ID
LEFT JOIN {{ source('football_data', 'competitions') }} comp
    ON c.DOMESTIC_COMPETITION_ID = comp.COMPETITION_ID