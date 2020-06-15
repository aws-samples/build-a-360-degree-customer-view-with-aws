## Let’s go back to the Lake formation workflow that extracts data from our relational database, to see it has finished.

Go to Lake Formation Workflows Blueprints, wait to see your workflow COMPLETED.

![bp 0](pic-wf00.png)


**Step 1:** Click on the sourcemf_full_workflow and then on the Run ID.

![bp 0](pic-wf01.png)


**Step 2:** Verify the completion of this workflow, that reads data from the relational database and then save it to Amazon S3.

![bp 1](pic-wf02.png)

For your ongoing database changes, you can create an Incremental Workflow and use an incremental column in your table as your bookmark key.




**Step 3:** Go to AWS Glue and see the table definition that was created with the full load and incremental data.

![bp 1](pic-wf03.png)

**Step 4:** Click Edit schema on the top right position for table sourcemf_sourcemf_public_transactions.


![bp 1](pic-wf04.png)


**Step 5:** Click on the date Data type field for column date to change it from timestamp to string.

![bp 1](pic-wf05.png)


**Step 6:** Select string for date column type. Because the timestamp format is not ok and has to be fixed with a job.

![bp 1](pic-wf06.png)

**Step 7:** Update and Save

**Step 8:** Confirm that you have saved the new schema, by checking if date column is updated to string column type, go to [table definition](https://us-west-2.console.aws.amazon.com/glue/home?region=us-west-2#table:name=sourcemf_sourcemf_public_transactions;namespace=c360view_raw).




## [PPerform transformation with relational database source raw tables and to have it transformed to parquet files.](../transdb/README.md)


## License

This library is licensed under the MIT-0 License. See the LICENSE file.
