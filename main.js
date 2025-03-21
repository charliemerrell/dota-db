const duckdb = require("duckdb");
const axios = require("axios");
const arrow = require("apache-arrow");
const path = require("path");

main();

async function main() {
    const db = CreateDatabaseIfNotExists();
    const con = db.connect();
    try {
        await ensureArrowInstalled(con);

        await CreateMatchesTableIfNotExists(con);
        await CreateHeroesTableIfNotExists(con);

        await refreshHeroesTable(db);

        // Fetch matches and insert them into the database
        while (true) {
            const maxMatchId = (await getMaxMatchId(con)) || "0";
            console.log("Max match ID:", maxMatchId);
            const matches = await getNextMatches(maxMatchId, 100000);
            if (matches.length === 0) {
                console.log("No more matches to fetch.");
                break;
            }
            await appendMatches(matches, db);
            console.log("Matches inserted successfully.");
        }
    } finally {
        con.close();
    }
}

function ensureArrowInstalled(con) {
    return new Promise((resolve, reject) => {
        con.run("INSTALL arrow; LOAD arrow;", (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

function CreateDatabaseIfNotExists() {
    return new duckdb.Database(path.join(__dirname, "dota.db"));
}

function CreateMatchesTableIfNotExists(con) {
    return new Promise((resolve, reject) => {
        const createTableQuery = `
        CREATE TABLE IF NOT EXISTS public_matches (
          match_id bigint PRIMARY KEY,
          radiant_win boolean,
          start_time integer,
          duration integer,
          lobby_type integer,
          game_mode integer,
          avg_rank_tier double precision,
          num_rank_tier integer,
          radiant_team integer[],
          dire_team integer[]
        );
        `;
        con.run(createTableQuery, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

function CreateHeroesTableIfNotExists(con) {
    return new Promise((resolve, reject) => {
        const createTableQuery = `
        CREATE TABLE IF NOT EXISTS heroes(
            id int PRIMARY KEY,
            name text,
            localized_name text,
            primary_attr text,
            attack_type text,
        );
        `;
        con.run(createTableQuery, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

async function queryOpenDota(sql) {
    const encodedSql = encodeURIComponent(sql);
    const url = `https://api.opendota.com/api/explorer?sql=${encodedSql}`;
    const response = await axios.get(url);
    return response.data.rows;
}

function getNextMatches(greaterThanMatchId, n) {
    const sql = `
        SELECT
            match_id,
            radiant_win,
            start_time,
            duration,
            lobby_type,
            game_mode,
            avg_rank_tier,
            num_rank_tier,
            radiant_team,
            dire_team
        FROM public_matches
        WHERE match_id > ${greaterThanMatchId}
        AND lobby_type = 7
        AND game_mode = 22
        ORDER BY match_id ASC
        LIMIT ${n}
    `;
    return queryOpenDota(sql);
}

function getMaxMatchId(con) {
    return new Promise((resolve, reject) => {
        con.all(
            `SELECT MAX(match_id) AS max FROM public_matches;`,
            (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    let maxMatchIdStr = rows[0]["max"].toString();
                    if (maxMatchIdStr.endsWith("n")) {
                        maxMatchIdStr = maxMatchIdStr.slice(0, -1);
                    }
                    resolve(maxMatchIdStr);
                }
            }
        );
    });
}

function appendMatches(matches, db) {
    return new Promise((resolve, reject) => {
        const arrowTable = arrow.tableFromJSON(matches);
        db.register_buffer(
            "arrow_matches",
            [arrow.tableToIPC(arrowTable)],
            true,
            (err, _) => {
                if (err) {
                    reject(err);
                    return;
                }
                db.exec(
                    `INSERT INTO public_matches SELECT * FROM arrow_matches;`,
                    (err) => {
                        if (err) {
                            reject(err);
                        } else {
                            resolve();
                        }
                    }
                );
            }
        );
    });
}

async function refreshHeroesTable(db) {
    // truncate heroes table
    await new Promise((resolve, reject) => {
        db.exec(`DELETE FROM heroes;`, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });

    const sql = `
        SELECT
            id,
            name,
            localized_name,
            primary_attr,
            attack_type
        FROM heroes
    `;
    const heroes = await queryOpenDota(sql);
    return new Promise((resolve, reject) => {
        const arrowTable = arrow.tableFromJSON(heroes);
        db.register_buffer(
            "arrow_heroes",
            [arrow.tableToIPC(arrowTable)],
            true,
            (err, _) => {
                if (err) {
                    reject(err);
                    return;
                }
                db.exec(
                    `INSERT INTO heroes SELECT * FROM arrow_heroes;`,
                    (err) => {
                        if (err) {
                            reject(err);
                        } else {
                            resolve();
                        }
                    }
                );
            }
        );
    });
}
