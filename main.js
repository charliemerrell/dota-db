const duckdb = require("duckdb");
const axios = require("axios");
const arrow = require("apache-arrow");

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
        ORDER BY match_id DESC
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

        // Install and load Arrow extension
        con.run(`INSTALL arrow; LOAD arrow;`, async (err) => {
            if (err) {
                console.warn(err);
                return;
            }

            // Fetch matches and insert them into the database
            const matches = await getNextMatches(0, 10);
            const arrowTable = arrow.tableFromJSON(matches);
            db.register_buffer(
                "matchesTable",
                [arrow.tableToIPC(arrowTable)],
                true,
                (err, res) => {
                    if (err) {
                        console.warn(err);
                        return;
                    }

                    // `SELECT * FROM matchesTable` would return the entries in `matches`
                    console.log("Matches inserted successfully.");
                }
            );
        });
    } finally {
        con.close();
    }
}
main();
