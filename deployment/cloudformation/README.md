## Deploy CloudFormation template to create basic infrastructure

**Step 1:** Open the AWS Cloudformation template.

![cf 1](pic-cf1.png)


**Step 2:** Choose Next and fill up the parameters:

* **Stack name:** c360view
*	**Network Configuration:**
 *	Which VPC should this be deployed to?: *Default VPC (172.31.0.0/16)*
 *	SubnetAz1: (172.31.0.0/20) (look for this IP range)
 *	SubnetAz2: (172.31.16.0/20) (look for this IP range)
*	**InstanceKeyPairParameter:** c360view-oregon (the one that you created before)

![cf 2](pic-cf2.png)


**Step 3:** Choose Next for the next window.


**Step 4:** Check the “I acknowledge that AWS CloudFormation might create IAM resources.”  and Create stack.

![cf 3](pic-cf3.png)

**Step 5:** Go to Resources tab to look for completion of resources.

![cf 4](pic-cf4.png)

The template has created the following resources to optimize your time.
3 Amazon S3 buckets:
*	RawDataS3Bucket
*	StageDataS3Bucket
*	AnalyticsDataS3Bucket

1 RDS instance with PostgreSQL database to simulate your transaction database.
*	RDSSource – sourcemf

6 Lambda functions to generate data for different source.
*	c360viewCRMApi
*	c360viewGetCRMApi
*	c360viewGetGaTables
*	c360viewMFgenAccount
*	c360viewMFgenCard
*	c360viewMFgenGBank
5 CloudWatch events schedules, to trigger the Lambda functions.

2 Security group, firewalls, one for the EC2 instance, other for Replication instances and Lambda functions.

1 Role for the AWS Lambda functions
1 Role for the AWS Glue

1 Amazon EMR cluster

Check the status of each resource, and order by resource status.


## Deploy [CloudFormation](cloudformation/README.md)





## License

This library is licensed under the MIT-0 License. See the LICENSE file.
