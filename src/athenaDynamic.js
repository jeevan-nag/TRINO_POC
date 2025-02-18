const { S3Client, ListObjectsCommand, GetObjectCommand } = require("@aws-sdk/client-s3");
const { AthenaClient, StartQueryExecutionCommand, GetQueryResultsCommand, GetQueryExecutionCommand } = require("@aws-sdk/client-athena");
const parquet = require("parquetjs-lite"); // Make sure to install this: npm install parquetjs-lite
const tmp = require("tmp");
const fs = require("fs");

// Configure Athena and S3 clients
const athena = new AthenaClient({
    region: "eu-north-1", // update as needed
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || ""
    }
});

const s3 = new S3Client({
    region: "eu-north-1",
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || ''
    }
}
);

/**
 * Infer a database name from the S3 location.
 * Example: For s3://my-bucket/data/, you might get "my_bucket_data"
 */
function inferDatabaseNameFromS3Location(s3Location) {
    try {
        const url = new URL(s3Location);
        const bucket = url.hostname;
        const pathParts = url.pathname.split("/").filter(Boolean);
        const lastFolder = pathParts[pathParts.length - 1] || "default";
        return `${bucket}_${lastFolder}`.replace(/-/g, "_");
    } catch (err) {
        console.error("Error inferring database name", err);
        return "default_db";
    }
}

/**
 * Infer a table name from the S3 location.
 * Example: For s3://my-bucket/data/, you might get "data"
 */
function inferTableNameFromS3Location(s3Location) {
    try {
        const url = new URL(s3Location);
        const pathParts = url.pathname.split("/").filter(Boolean);
        const tableName = pathParts[pathParts.length - 1] || "default_table";
        return tableName.replace(/-/g, "_");
    } catch (err) {
        console.error("Error inferring table name", err);
        return "default_table";
    }
}

/**
 * Infers the Parquet schema by:
 *  - Listing objects in the S3 prefix
 *  - Picking one Parquet file
 *  - Reading its schema using parquetjs-lite
 *  - Mapping field types to Athena types (this example uses a simple mapping)
 */
async function inferParquetSchema(s3Location) {
    // Parse bucket and prefix from the S3 URL
    const url = new URL(s3Location);
    const Bucket = url.hostname;
    const Prefix = url.pathname.slice(1); // remove leading "/"

    // List objects in the specified S3 location
    const listParams = { Bucket, Prefix };
    const listResponse = await s3.send(new ListObjectsCommand(listParams));
    if (!listResponse.Contents) {
        throw new Error("No objects found at the specified S3 location.");
    }
    const parquetFileObj = listResponse.Contents.find(obj => obj.Key.endsWith(".parquet"));
    if (!parquetFileObj) {
        throw new Error("No Parquet file found in location: " + s3Location);
    }

    // Get the Parquet file from S3
    const getObjectParams = { Bucket, Key: parquetFileObj.Key };
    const getObjectResponse = await s3.send(new GetObjectCommand(getObjectParams));

    // Convert the stream to a buffer
    const streamToBuffer = (stream) =>
        new Promise((resolve, reject) => {
            const chunks = [];
            stream.on("data", chunk => chunks.push(chunk));
            stream.on("end", () => resolve(Buffer.concat(chunks)));
            stream.on("error", reject);
        });
    const buffer = await streamToBuffer(getObjectResponse.Body);

    // Write the buffer to a temporary file so parquetjs can open it
    const tmpFile = tmp.fileSync({ postfix: ".parquet" });
    fs.writeFileSync(tmpFile.name, buffer);

    // Open the Parquet file and read its schema
    const reader = await parquet.ParquetReader.openFile(tmpFile.name);
    const schema = reader.getSchema();
    await reader.close();
    tmpFile.removeCallback(); // Clean up temporary file

    // Map the Parquet fields to Athena column definitions
    // Now we handle repeated fields (arrays) explicitly.
    let columnsDDL = [];
    for (const [fieldName, fieldInfo] of Object.entries(schema.fields)) {
        // Determine the base Athena type
        let baseType = "STRING"; // default type
        if (
            fieldInfo.originalType === "INT_64" ||
            fieldInfo.primitiveType === "INT64" ||
            fieldInfo.primitiveType === "INT32"
        ) {
            baseType = "BIGINT";
        } else if (
            fieldInfo.primitiveType === "DOUBLE" ||
            fieldInfo.primitiveType === "FLOAT"
        ) {
            baseType = "DOUBLE";
        } else if (fieldInfo.primitiveType === "BOOLEAN") {
            baseType = "BOOLEAN";
        }

        // If the field is repeated, map it to an array type
        let athenaType = baseType;
        if (fieldInfo.repetitionType && fieldInfo.repetitionType.toUpperCase() === "REPEATED") {
            athenaType = `array<${baseType.toLowerCase()}>`;
        }

        columnsDDL.push(`\`${fieldName}\` ${athenaType}`);
    }
    return columnsDDL.join(",\n");
}

/**
 * Creates the database if it doesn't exist.
 */
async function createDatabase(database, outputBucket) {
    const createDbQuery = `CREATE DATABASE IF NOT EXISTS ${database};`;
    await executeAthenaQuery(createDbQuery, "default", outputBucket);
}

/**
 * Executes an Athena query and waits for the result.
 */
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

        // Wait for the query to finish
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

/**
 * Polls Athena until the query finishes.
 */
async function waitForQueryToFinish(queryExecutionId) {
    while (true) {
        try {
            const statusCommand = new GetQueryExecutionCommand({ QueryExecutionId: queryExecutionId });
            const statusResult = await athena.send(statusCommand);
            const status = statusResult.QueryExecution.Status.State;
            console.log(`Query Status: ${status}`);

            if (status === "SUCCEEDED") return;
            if (status === "FAILED" || status === "CANCELLED") {
                console.error("Query Failed. Reason:", statusResult.QueryExecution.Status.StateChangeReason);
                throw new Error(`Query failed with status: ${status}`);
            }
            await new Promise(resolve => setTimeout(resolve, 5000)); // Poll every 5 seconds
        } catch (error) {
            console.error("Error checking query status:", error);
            throw error;
        }
    }
}

/**
 * Main function that:
 *  - Reads dynamic inputs (S3 location, output bucket)
 *  - Infers the database, table, and schema
 *  - Creates the database/table in Athena and runs a sample query
 */
const runDynamicAthenaQuery = async () => {
    // Dynamic inputs: change these via environment variables or pass as parameters
    const s3Location = process.env.S3_LOCATION || "s3://hiring-from-cafe/parquet/";
    const outputBucket = process.env.OUTPUT_BUCKET || "s3://hiring-from-cafe/parquet_output/";

    // Infer database and table names from the S3 location
    const database = inferDatabaseNameFromS3Location(s3Location);
    const table = inferTableNameFromS3Location(s3Location);
    console.log(`Inferred Database: ${database}, Table: ${table}`);

    // Infer the schema (columns) from one of the parquet files in the S3 location
    const columnsDDL = await inferParquetSchema(s3Location);
    console.log("Inferred Columns for Athena Table:\n", columnsDDL);

    // Create the database if it doesn't exist
    await createDatabase(database, outputBucket);

    // Create the Athena table dynamically using the inferred schema
    const createTableQuery = `
    CREATE EXTERNAL TABLE IF NOT EXISTS ${table} (
      ${columnsDDL}
    )
    STORED AS PARQUET
    LOCATION '${s3Location}';
  `;
    console.log("Executing Create Table Query:\n", createTableQuery);
    await executeAthenaQuery(createTableQuery, database, outputBucket);

    // Run a sample SELECT query on the new table
    const query = `SELECT * FROM ${table} LIMIT 10;`;
    console.log("Executing Sample Query:\n", query);
    await executeAthenaQuery(query, database, outputBucket);
}

module.exports = { runDynamicAthenaQuery }

