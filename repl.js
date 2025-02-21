const readline = require("readline");
const path = require("path");
const duckdb = require("duckdb");

const db = new duckdb.Database(path.join(__dirname, "dota.db"));

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "dota-db> ",
});

console.log("Welcome to the Dota DB CLI REPL");
rl.prompt();

rl.on("line", (line) => {
    const query = line.trim();
    if (query.toLowerCase() === "exit") {
        rl.close();
        return;
    }

    db.all(query, (err, rows) => {
        if (err) {
            console.error("Error:", err.message);
        } else {
            const serializedRows = rows.map((row) => {
                for (const key in row) {
                    if (typeof row[key] === "bigint") {
                        row[key] = row[key].toString();
                    }
                }
                return row;
            });
            console.table(serializedRows);
        }
        rl.prompt();
    });
}).on("close", () => {
    console.log("Exiting Dota DB CLI REPL");
    process.exit(0);
});
