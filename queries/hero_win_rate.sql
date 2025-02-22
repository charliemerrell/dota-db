WITH hero_match_results AS (
    SELECT
        H.localized_name,
        CASE
            WHEN M.radiant_win = TRUE AND H.Id IN M.radiant_team THEN 'Win'
            WHEN M.radiant_win = FALSE AND H.Id IN M.dire_team THEN 'Win'
            ELSE 'Loss'
        END AS Result
    FROM heroes AS H
    INNER JOIN public_matches AS M
    ON H.Id IN M.radiant_team
    OR H.Id IN M.dire_team
)
SELECT
    localized_name,
    COUNT(*) FILTER (WHERE Result = 'Win') AS Wins,
    COUNT(*) FILTER (WHERE Result = 'Loss') AS Losses,
    COUNT(*) AS Total,
    ROUND(COUNT(*) FILTER (WHERE Result = 'Win') * 100.0 / COUNT(*), 2) AS Win_Rate
FROM hero_match_results
GROUP BY localized_name
ORDER BY Win_Rate DESC;
