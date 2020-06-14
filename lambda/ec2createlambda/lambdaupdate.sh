#/bin/bash
sudo yum install gcc -y
sudo yum install python3.7 -y
sudo yum install postgresql-devel -y
mkdir lambda-pg-generator
cd lambda-pg-generator
pip3 install psycopg2-binary -t .
wget https://raw.githubusercontent.com/aws-samples/build-a-360-degree-customer-view-with-aws/master/lambda/lambda-pg-generator/postgresql.py
zip -r ../newlambda.zip postgresql.py psycopg2*
aws lambda update-function-code --function-name c360viewIngestionPostgresql \
--zip-file fileb://../newlambda.zip --region us-west-2
