## Populate an Amazon DynamoDB table with the results to be the source for low latency queries from your applications or APIs.

We are going to use a Hive script to perform the query on source table and save it DynamoDB.

**Step 1:** Go to [EMR console](https://us-west-2.console.aws.amazon.com/elasticmapreduce/home?region=us-west-2).

![bp 1](pic-ddb01.png)


**Step 2:** click on c360cluster.

![bp 1](pic-ddb02.png)

**Step 3:** click on Steps tab.

![bp 1](pic-ddb03.png)

**Step 4:** Add step.
*	**Step type:** Hive program
*	**Name:** loadtodynamodb
*	**Script S3 location:** s3://<your_stage_bucket>/ library/c360dynamodbload.q
Use the bucket browser to select the application location.


![bp 1](pic-ddb04.png)

*	**Input S3 location:** leave blank
*	**Output s3 location:** leave blank
*	**Arguments:** leave blank
*	**Action on failure:** continue


![bp 1](pic-ddb05.png)


**Step 5:** check the job status, going from pending to running.

![bp 1](pic-ddb06.png)


**Step 6:** Check all applications status on the Application history tab.

![bp 1](pic-ddb07.png)


## [Now you can query data from Amazon DynamoDB.](../viewddb/README.md)


## License

This library is licensed under the MIT-0 License. See the LICENSE file.
