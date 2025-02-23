WITH matches AS ( 
    SELECT
        *
    FROM public_matches
    -- can add limit or filter to make query faster
),
individual_hero_results AS (
    SELECT
        H.id,
        CASE
            WHEN M.radiant_win = TRUE AND H.Id IN M.radiant_team THEN 'Win'
            WHEN M.radiant_win = FALSE AND H.Id IN M.dire_team THEN 'Win'
            ELSE 'Loss'
        END AS result
    FROM heroes AS H
    INNER JOIN matches AS M
    ON H.Id IN M.radiant_team
    OR H.Id IN M.dire_team
),
individual_hero_win_rates AS (
    SELECT
        id,
        ROUND(COUNT(*) FILTER (WHERE result = 'Win') * 100.0 / COUNT(*), 2) AS win_rate
    FROM individual_hero_results
    GROUP BY id
),
hero_pairs AS (
    SELECT
        H1.Id AS id1,
        H2.Id AS id2,
    FROM heroes AS H1
    CROSS JOIN heroes AS H2
    WHERE H1.Id < H2.Id
),
pair_results AS (
    SELECT
        HP.id1,
        HP.id2,
        CASE
            WHEN M.radiant_win = TRUE AND HP.id1 IN M.radiant_team THEN 'Win'
            WHEN M.radiant_win = FALSE AND HP.id1 IN M.dire_team THEN 'Win'
            ELSE 'Loss'
        END AS result
    FROM hero_pairs AS HP
    INNER JOIN matches AS M
    ON (HP.id1 IN M.radiant_team AND HP.id2 IN M.radiant_team)
    OR (HP.id1 IN M.dire_team AND HP.id2 IN M.dire_team)
),
pair_win_rates AS (
    SELECT
        id1,
        id2,
        ROUND(COUNT(*) FILTER (WHERE result = 'Win') * 100.0 / COUNT(*), 2) AS win_rate,
        COUNT(*) AS total
    FROM pair_results
    GROUP BY id1, id2
    HAVING total > 10000 -- filter out pairs with low total matches
),
pair_synergy AS (
    SELECT
        PWR.id1,
        PWR.id2,
        PWR.win_rate AS pair_win_rate,
        IHW1.win_rate AS hero1_win_rate,
        IHW2.win_rate AS hero2_win_rate,
        (IHW1.win_rate + IHW2.win_rate) / 2 AS expected_win_rate,
        PWR.win_rate - (IHW1.win_rate + IHW2.win_rate) / 2 AS synergy,
        PWR.total AS total
    FROM pair_win_rates AS PWR
    INNER JOIN individual_hero_win_rates AS IHW1
    ON PWR.id1 = IHW1.id
    INNER JOIN individual_hero_win_rates AS IHW2
    ON PWR.id2 = IHW2.id
)
SELECT 
    H1.localized_name AS hero1,
    H2.localized_name AS hero2,
    pair_win_rate,
    hero1_win_rate,
    hero2_win_rate,
    expected_win_rate,
    synergy,
    total
FROM pair_synergy
INNER JOIN heroes AS H1
ON pair_synergy.id1 = H1.id
INNER JOIN heroes AS H2
ON pair_synergy.id2 = H2.id
ORDER BY synergy DESC;