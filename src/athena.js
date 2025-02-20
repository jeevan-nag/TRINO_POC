const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const { AthenaClient, StartQueryExecutionCommand, GetQueryResultsCommand, GetQueryExecutionCommand } = require("@aws-sdk/client-athena");

const athena = new AthenaClient({
    region: "us-west-2", // Update to the correct region
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
    const database = "hiring"; // Update this with your Athena database name
    const outputBucket = "s3://hiring-results"; // S3 bucket to store query results
    await createDatabase(database, outputBucket);
    // Create Athena Table(DDL for Parquet File)
    const UnformattedQuery = `
       CREATE EXTERNAL TABLE IF NOT EXISTS  hiring_unfiltered_results(
            v5_processed_job_data STRUCT<technical_tools: ARRAY<STRING>>,
            v5_processed_company_data STRUCT<website: STRING>
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        WITH SERDEPROPERTIES ("ignore.malformed.json" = "true")
        LOCATION 's3://hiring-from-cafe/jobs/'
        TBLPROPERTIES ('has_encrypted_data'='false')
    `;
    await executeAthenaQuery(UnformattedQuery, database, outputBucket + '/Unformatted');

    const formattedQueryQuery = `CREATE TABLE hiring_filtered_results
            WITH (
                format = 'PARQUET',  -- Use a columnar format for better performance
                external_location = '${outputBucket + '/Formatted'}',
                write_compression = 'SNAPPY'
            ) AS 
            SELECT  tech_tool AS technology_name, v5_processed_company_data.website AS company_domain
            FROM hiring.hiring_unfiltered_results
            CROSS JOIN UNNEST(v5_processed_job_data.technical_tools) AS t(tech_tool)  
            WHERE  v5_processed_company_data.website IS NOT NULL`


    // Execute the CREATE TABLE query in Athena
    await executeAthenaQuery(formattedQueryQuery, database, outputBucket + '/Formatted');

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
