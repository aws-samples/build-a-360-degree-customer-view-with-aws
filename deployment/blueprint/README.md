## Create a connection for relational database as source.


**Step 1:** Go to [AWS Glue Database Connections](https://us-west-2.console.aws.amazon.com/glue/home?region=us-west-2#catalog:tab=connections), and Add connection.

![bp 0](pic-bp00.png)

*	**Connection name:** sourcemf
*	**Connection type:** Amazon RDS
*	**Database Engine:** PostgreSQL



![cf 1](pic-bp01.png)


**Step 2:** Set up access, choosing your Instance sourcemf:

*	**Instance:** sourcemf
*	**Database name:** sourcemf
*	**Username:** sourcemf
*	**Password:** Tim3t0change

![cf 2](pic-bp02.png)


**Step 3:** Click on **Finish:**

![cf 3](pic-bp03.png)





**Step 1:** Go to [Amazon S3](https://s3.console.aws.amazon.com/s3/home?region=us-west-2) console to check the data in your raw bucket.
Go to Amazon S3 console to check the data in your raw bucket.

Search for c360view buckets.

![sw 0](pic-sw00.png)


**Step 2:** Click on the c360view-us-west-2-<YOUR_ACCOUNT_ID>-raw bucket.
Refresh the bucket while the lambda codes are running.

![sw 1](pic-sw01.png)


**Step 3:** Enter inside each folder to check the files created inside.

![sw 2](pic-sw02.png)

Account data, at account folder.

![sw 3](pic-sw03.png)

GA sessions data, at data/GA/ga_session_20YYMMDD folders.


**Step 4:**

**Step 5:**


## Setup [Lake Formation](../lakeformation/README.md)


## License

This library is licensed under the MIT-0 License. See the LICENSE file.
