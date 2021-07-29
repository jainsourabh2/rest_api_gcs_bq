//Importing required Libraries 
const axios = require('axios');
const {BigQuery} = require('@google-cloud/bigquery');
const { Parser } = require('json2csv');
const {Storage} = require('@google-cloud/storage');

//Declaring variables
let url = `https://reqres.in/api/users?page=1`;
let datasetID = '1devjam_demo'
let tableID = 'users'
let bucketName = 'sourabhjainceanalytics';
let fileName = 'restapi/users.csv'

let fields = ["id", "email", "first_name", "last_name", "avatar"];
let parser = new Parser({
    fields,
    unwind: fields
});

const metadata = {
    sourceFormat: 'CSV',
    skipLeadingRows: 1,
    schema: {
      fields: [
        {name: 'id', type: 'INT64'},
        {name: 'email', type: 'STRING'},
        {name: 'first_name', type: 'STRING'},
        {name: 'last_name', type: 'STRING'},
        {name: 'avatar', type: 'STRING'}
    ],
    },
    location: 'US',
  };

//using axios library to invoke the REST API.
axios({
    method:'get',
    url,
})
.then(function (response) {
    
    // Fetching the data response into a variable. 
    let records = response.data.data;

    //Convert the JSON into CSV 
    const content_csv = parser.parse(records);

    // Loading the CSV into Google Cloud Storage
    const storage = new Storage();
    const myBucket = storage.bucket(bucketName);
    const file = myBucket.file(fileName);

    //Save the file into Google Cloud Storage
    file.save(content_csv, function(err) {
        if (!err) {
          console.log("File saved in bucket successfully");
        }
    });

    // Loading the CSV data into BigQuery
    async function load_bq () {
    const bigquery = new BigQuery();
    const [job] = await bigquery
    .dataset(datasetID)
    .table(tableID)
    .load(storage.bucket(bucketName).file(fileName), metadata);

    // Uncomment below line for google cloud functions
    // Also make the function export.hellowWorld = (req,resp) as export.hellowWorld = async (req,resp) . Add the word async    
    // resp.send({message:'Success'})
    };

    load_bq();
})
// Displaying error if any.
.catch(function (error) {
    console.log(error);
});