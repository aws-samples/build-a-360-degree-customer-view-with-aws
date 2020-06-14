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


**Step 4:** Edit your connection to Add a Security group to your connection:

![cf 4](pic-bp04.png)

**Step 5:** Check the c360view-c360-Access and c360view-RDS-Source security group:
* **Group name:** c360view-c360-Access and c360view-RDS-Source
* **Password:** Tim3t0change (re-enter it)

![cf 5](pic-bp05.png)

**Step 6:** Test your connection:
*	**IAM role:** Glue-role-c360view

![cf 6](pic-bp06.png)


**Step 7:** You will see sourcemf connected successfully:

![cf 7](pic-bp07.png)

If you receive the following error:

![cf 8](pic-bp08.png)

Edit your connection again and change the Subnet to another one.

![cf 9](pic-bp09.png)

Then test the connection with the new chosen subnet until it works.

## Setup [Lake Formation](../lakeformation/README.md)


## License

This library is licensed under the MIT-0 License. See the LICENSE file.
