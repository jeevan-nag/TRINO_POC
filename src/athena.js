const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const { AthenaClient, StartQueryExecutionCommand, GetQueryResultsCommand, GetQueryExecutionCommand } = require("@aws-sdk/client-athena");

const athena = new AthenaClient({
    region: "eu-north-1", // Update to the correct region
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || ''
    }
});

async function createDatabase(database, outputBucket) {
    const createDbQuery = `CREATE DATABASE IF NOT EXISTS ${database};`;
    await executeAthenaQuery(createDbQuery, "default", outputBucket);
}
// Function to create Athena table and query
async function runAthenaQuery() {
    const database = "parquet_results"; // Update this with your Athena database name
    const outputBucket = "s3://hiring-from-cafe/parquet_output/"; // S3 bucket to store query results
    await createDatabase(database, outputBucket);
    // Create Athena Table (DDL for Parquet File)
    const createTableQuery = `
        CREATE EXTERNAL TABLE IF NOT EXISTS my_parquet_table (
            technical_tools array<string>,
            company_domain STRING
        )
        STORED AS PARQUET
        LOCATION 's3://hiring-from-cafe/parquet/';
    `;

    // Execute the CREATE TABLE query in Athena
    await executeAthenaQuery(createTableQuery, database, outputBucket);

    // Run a query on the table
    const query = `SELECT * FROM my_parquet_table LIMIT 10;`;

    // Execute the SELECT query in Athena
    await executeAthenaQuery(query, database, outputBucket);
}

// Function to execute Athena query
async function executeAthenaQuery(query, database, outputBucket) {
    const queryExecutionParams = {
        QueryString: query,
        QueryExecutionContext: {
            Database: database
        },
        ResultConfiguration: {
            OutputLocation: outputBucket
        }
    };

    try {
        const startQueryCommand = new StartQueryExecutionCommand(queryExecutionParams);
        const queryExecutionResult = await athena.send(startQueryCommand);

        console.log("Query started successfully:", queryExecutionResult.QueryExecutionId);

        // Wait for the query to finish (polling for result)
        await waitForQueryToFinish(queryExecutionResult.QueryExecutionId);

        // Fetch query results
        const getResultsParams = {
            QueryExecutionId: queryExecutionResult.QueryExecutionId
        };
        const resultsCommand = new GetQueryResultsCommand(getResultsParams);
        const results = await athena.send(resultsCommand);

        console.log("Query results:", JSON.stringify(results, null, 2));
    } catch (error) {
        console.error("Error executing query:", error);
    }
}
async function waitForQueryToFinish(queryExecutionId) {
    while (true) {
        try {
            const statusCommand = new GetQueryExecutionCommand({ QueryExecutionId: queryExecutionId });
            const statusResult = await athena.send(statusCommand);
            const status = statusResult.QueryExecution.Status.State;

            console.log(`Query Status: ${status}`);

            if (status === "SUCCEEDED") return;
            if (status === "FAILED" || status === "CANCELLED") {
                console.error("❌ Query Failed. Reason:", statusResult.QueryExecution.Status.StateChangeReason);
                throw new Error(`Query failed with status: ${status}`);
            }

            await new Promise(resolve => setTimeout(resolve, 5000)); // Poll every 5 seconds
        } catch (error) {
            console.error("Error checking query status:", error);
            throw error;
        }
    }
}

// Run the Athena query execution
runAthenaQuery().catch(console.error);
