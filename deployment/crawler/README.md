## Execute a crawler at AWS Glue Console to get tables definitions from your raw bucket.


**Step 1:** Go to AWS [Glue Crawlers](https://us-west-2.console.aws.amazon.com/glue/home?region=us-west-2#catalog:tab=crawlers):

![bp 0](pic-cw00.png)



**Step 2:** Click on the Crawler360rawdata and Run crawler.

*	•	This crawler will read your raw bucket files and create table definitions for each of the table schema for database c360view-raw.


![bp 1](pic-cw01.png)



**Step 3:** It will show five tables added.

![cf 2](pic-cw02.png)


**Step 4:** You should see 5 tables added by Crawler.

![cf 3](pic-cw03.png)

**Step 5:** You can verify them going to Database -> Tables in the same console.

![cf 5](pic-cw04.png)

**Step 6:** Select Crawlerc360visitorid and run crawler.

![cf 5](pic-cw05.png)


You should see one table added by the crawler.

![cf 5](pic-cw06.png)


## Setup [Execute a crawler at AWS Glue Console to get tables definitions from your raw bucket](../crawler/README.md)


## License

This library is licensed under the MIT-0 License. See the LICENSE file.
