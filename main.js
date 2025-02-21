const duckdb = require("duckdb");

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

const db = CreateDatabaseIfNotExists();
const con = db.connect();
try {
    CreateTableIfNotExists(con);
} finally {
    con.close();
}
