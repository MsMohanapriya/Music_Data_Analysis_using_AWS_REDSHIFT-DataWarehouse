# MUsic data analysis using Data Engineering

Analysing the subset of Million Song Dataset 

First collecting the dataset from the available resource.

The files are stored in the Amazon S3 bucket using command line interface.

The data in the S3 is used for further analysis

A connection is established between a s3_bucket and redshift, where the s3_bucket data are stored in the database.

we use AWS glue to create ETL pipeline and perform ETL jobs automatically and store the output in the specified location such as in redshit warehouse.

Then sql queries are used to query from the database along with the python code

Finally we can done visualization in AWS quickSight.
