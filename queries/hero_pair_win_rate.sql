WITH hero_pairs AS (
    SELECT
        H1.Id AS Id1,
        H2.Id AS Id2,
        H1.localized_name || ' and ' || H2.localized_name AS Pair
    FROM heroes AS H1
    CROSS JOIN heroes AS H2
    WHERE H1.Id < H2.Id
),
match_results AS (
    SELECT
        HP.Pair,
        CASE
            WHEN M.radiant_win = TRUE AND HP.Id1 IN M.radiant_team THEN 'Win'
            WHEN M.radiant_win = FALSE AND HP.Id1 IN M.dire_team THEN 'Win'
            ELSE 'Loss'
        END AS Result
    FROM hero_pairs AS HP
    INNER JOIN public_matches AS M
    ON (HP.Id1 IN M.radiant_team AND HP.Id2 IN M.radiant_team)
    OR (HP.Id1 IN M.dire_team AND HP.Id2 IN M.dire_team)
)
SELECT
    Pair,
    COUNT(*) FILTER (WHERE Result = 'Win') AS Wins,
    COUNT(*) FILTER (WHERE Result = 'Loss') AS Losses,
    COUNT(*) AS Total,
    ROUND(COUNT(*) FILTER (WHERE Result = 'Win') * 100.0 / COUNT(*), 2) AS Win_Rate
FROM match_results
GROUP BY Pair
ORDER BY Win_Rate DESC;
