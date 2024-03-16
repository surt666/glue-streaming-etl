# Glue Kinesis Streaming Job Template with Custom Glue Job

This template creates a Kinesis Stream and a Glue Job that transforms data from a Kinesis Stream, and writes to an S3 bucket. 
The resulting data in S3 is in snappy parquet
Also creates an IoT rule that delivers to the stream. If your source is somethin else just delete it

The following input parameters are needed. Only lowercase letters, digits and underscore is allowed. No - as Glue does not allow it

Context parameters:\
ProjectName=myproject (will be used in naming of all services, follows S3 naming convention) \

Optional:\
Logging=false (if false log group will not be created, default true)

The core of this is the custom job that flattens the incoming stream data. This requires some Spark-Fu, so look in src/job.py to see an example of this

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth -c ProjectName=streamtest
```

```
$ cdk deploy -c ProjectName=streamtest
```
