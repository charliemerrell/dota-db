SELECT H.localized_name, COUNT(*) AS Count
FROM heroes AS H
INNER JOIN public_matches AS M
ON H.Id IN M.radiant_team
OR H.Id IN M.dire_team
GROUP BY H.localized_name;