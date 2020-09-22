## Build a 360-degree customer view in AWS using a powerful set of analytics tools

This git demonstrated how to bring data from different data systems as a set of customer dimensions and build a 360-degree customer view as a baseline for all customer analytics initiatives.

## Dimensions for a 360-degree customer view

In this git we will explore a hypothetic financial services company, as there are common dimensions for this industry and some dimensions that are also valid for any service industry, like marketing and communications, customer history or demographic dimension.

## Solution details

We created three buckets, one for each data purpose: raw data S3 bucket, stage data S3 bucket and analytics data S3 bucket. You can find in our git the deployment guide for the solution.
All the data besides Google Analytics sample from Kaggle were synthetic created using lambda functions using random range values. You can use Amazon AppFlow to extract your own data from Google Analytics data as described in this other blog post.
For the bank transactions we created a relational database Amazon RDS PostgreSQL.
To simulate the API we created Lambda function that responds as a CRM API, you can also use Amazon AppFlow to extract data from your Salesforce environment.

For the mainframe simulation the lambdas generate flat files on Amazon S3.

We then used Amazon CloudWatch events schedules, to trigger the Lambda functions.





## To deploy the example in your account go to [Deployment](deployment/README.md)



## License

This library is licensed under the MIT-0 License. See the LICENSE file.
