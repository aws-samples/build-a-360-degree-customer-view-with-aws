## Setup your Data Lake with Lake Formation


**Step 1:** Go to the [Lake Formation console](Go to the Lake Formation console:):

![lf 0](pic-lf00.png)


If this first screen doesn’t open like this go to Permissions -> Admins and database creators, and grant your user as administrator.


**Step 2:** Add your user as Data Lake admin, so your user can administer storage areas, databases and tables.

![cf 1](pic-lf01.png)

Add your user as Administrator and Save


**Step 3:** Add the buckets starting with “c360view” as [data lake locations](https://us-west-2.console.aws.amazon.com/lakeformation/home?region=us-west-2#register-list) for Lake Formation. Indicating there are part of your data lake.

![cf 2](pic-lf02.png)

**Step 4:** Click Register location.

![cf 3](pic-lf03.png)


**Step 5:** Click on Browse and select each c360view bucket to register.

![cf 4](pic-lf04.png)

Repeat it for the 3 buckets:
*	c360view-us-west-2-<your_account_id>-raw
*	c360view-us-west-2-<your_account_id>-stage
*	c360view-us-west-2-<your_account_id>-analytics
After registration you will see a screen like the following.

![cf 5](pic-lf05.png)


**Step 6:** On Lake formation data locations permission console Click on Grant to grant access to the AWS Lambda, AWS Glue service role and Amazon EMR EC2 role.

![cf 6](pic-lf06.png)

*	IAM users and roles: **c360-LambdaExecutionRole**
#### Storage locations:
  *	c360view-us-west-2-<your_account_id>-raw
  *	c360view-us-west-2-<your_account_id>-stage
  *	c360view-us-west-2-<your_account_id>-analytics

![cf 7](pic-lf07.png)

**Step 7:** Grant same locations to **Glue-role-c360view**.
*	IAM users and roles: **Glue-role-c360view**
#### Storage locations:
  *	c360view-us-west-2-<your_account_id>-raw
  *	c360view-us-west-2-<your_account_id>-stage
  *	c360view-us-west-2-<your_account_id>-analytics

![cf 8](pic-lf08.png)

**Step 8:** Grant same locations to **c360view-emrEc2Role**
•	IAM users and roles: **c360view-emrEc2Role**.
#### Storage locations:
o	c360view-us-west-2-<your_account_id>-raw
o	c360view-us-west-2-<your_account_id>-stage
o	c360view-us-west-2-<your_account_id>-analytics

![cf 9](pic-lf09.png)




## Now [Activate schedules](../schedules/README.md)


## License

This library is licensed under the MIT-0 License. See the LICENSE file.
