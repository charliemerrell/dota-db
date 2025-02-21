const duckdb = require("duckdb");
const axios = require("axios");
const arrow = require("apache-arrow");
const util = require("util");

function CreateDatabaseIfNotExists() {
    return new duckdb.Database(":memory:");
}

function CreateTableIfNotExists(con) {
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
            console.error("Error creating table:", err.message);
        } else {
            console.log("Table created successfully.");
        }
    });
}

async function getNextMatches(greaterThanMatchId, n) {
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
    const encodedSql = encodeURIComponent(sql);
    const url = `https://api.opendota.com/api/explorer?sql=${encodedSql}`;
    const response = await axios.get(url);
    return response.data.rows;
}

async function main() {
    const db = CreateDatabaseIfNotExists();
    const con = db.connect();
    try {
        CreateTableIfNotExists(con);

        const runAsync = util.promisify(con.run.bind(con));

        try {
            // Install and load Arrow extension
            await runAsync(`INSTALL arrow; LOAD arrow;`);

            // Fetch matches and insert them into the database
            for (let i = 0; i < 10; i++) {
                const maxMatchId = (await getMaxMatchId(con)) || 0;
                let maxMatchIdStr = maxMatchId.toString();
                if (maxMatchIdStr.endsWith('n')) {
                    maxMatchIdStr = maxMatchIdStr.slice(0, -1);
                }
                console.log("Max match ID:", maxMatchIdStr);
                const matches = await getNextMatches(maxMatchIdStr, 10);
                if (matches.length === 0) {
                    console.log("No more matches to fetch.");
                    break;
                }
                await appendMatches(matches, db);
                console.log("Matches inserted successfully.");
            }
        } catch (err) {
            console.warn(err);
        }
    } finally {
        con.close();
    }
}
main();

function getMaxMatchId(con) {
    return new Promise((resolve, reject) => {
        con.all(`SELECT MAX(match_id) AS max FROM public_matches;`, (err, rows) => {
            if (err) {
                reject(err);
            } else {
                console.log(rows)
                resolve(rows[0]["max"]);
            }
        });
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
