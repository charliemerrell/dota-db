# Dota db

## Setup

-   `npm i`
-   `curl https://install.duckdb.org | sh`

## Getting data

-   Run `main.js` to populate DB with all the matches on opendota (if you already have data, it'll append anything new).
    You can't run whilst connected the CLI (see below).

## CLI

-   `duckdb dota.db` to open
-   `Ctrl` + `D` to exit
-   `.maxrows 200` useful so you can see all heroes
-   `.timer on` useful to see query time
