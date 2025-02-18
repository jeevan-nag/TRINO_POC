const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const parquet = require("parquetjs");
const fs = require("fs");
const path = require("path");
const { v4 } = require("uuid"); // For unique filenames

// AWS S3 Configuration
const s3 = new S3Client({
    region: "eu-north-1", // Change to your AWS region
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || ''
    }
});

// Function to convert JSON to Parquet & upload to S3
const jsonToParquetAndUpload = async (jsonData, bucketName, s3Key) => {
    try {
        const tmpDir = path.join(__dirname, "tmp");
        if (!fs.existsSync(tmpDir)) {
            fs.mkdirSync(tmpDir);
        }
        const schema = new parquet.ParquetSchema({
            company_domain: { type: "UTF8" },
            technical_tools: { type: "UTF8", repeated: true }
        });
        const filePath = path.join(__dirname, `/tmp/${v4()}.parquet`); // Temporary file path
        // Write Parquet file
        const writer = await parquet.ParquetWriter.openFile(schema, filePath);
        for (const record of jsonData) {
            await writer.appendRow(record);
        }
        await writer.close();

        // Read file and upload to S3
        const fileBuffer = fs.readFileSync(filePath);
        const uploadParams = {
            Bucket: bucketName,
            Key: s3Key,
            Body: fileBuffer,
            endpoint: "https://s3.us-west-2.amazonaws.com",
            ContentType: "application/octet-stream"
        };

        await s3.send(new PutObjectCommand(uploadParams));

        console.log(`Successfully uploaded ${s3Key} to ${bucketName}`);

        // Clean up: delete the temporary file
        fs.unlinkSync(filePath);
        return true;
    } catch (err) {
        console.error("Error during conversion/upload:", err);
        throw err;
    }
}

module.exports = { jsonToParquetAndUpload }




