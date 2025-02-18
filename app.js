const express = require('express');
const app = express();
app.use(express.json());
const s3 = require("./src/s3");
const athenaDynamic = require("./src/athenaDynamic");
const dummyData = require("./dummyData.json")
const PORT = 8080

app.post("/jsonToParquetAndUploadToS3", async (req, res) => {
    try {
        let { bucketName = '', s3Key = '', jsonData = '' } = req.body;
        jsonData = jsonData || dummyData;
        const response = await s3.jsonToParquetAndUpload(jsonData, bucketName, s3Key);
        return res.status(200).send({ success: response, message: 'Success' })
    } catch (error) {
        return res.status(500).send({ success: false, message: error })
    }
})

app.post("/runDynamicAthenaQuery", async (req, res) => {
    try {
        const response = await athenaDynamic.runDynamicAthenaQuery();
        return res.status(200).send({ success: response, message: 'Success' })
    } catch (error) {
        return res.status(500).send({ success: false, message: error })
    }
})

app.listen(PORT, () => {
    console.log('Server is running at PORT', PORT);
})