# S3 to Redshift Snowplow Loader

To load historical event dumps from S3 back do Redshift Snowplow db.

## Usage

The `load.rb` script requires following ENV variables to be set:

- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_REGION
- REDSHIFT_URI

and it takes 2 arguments, the start and end date for historical data. The default for start date is 30 days ago and for end date it's today.

Example:

```sh
AWS_ACCESS_KEY_ID=aws-access-key AWS_SECRET_ACCESS_KEY=aws-secret-access-key AWS_REGION=eu-west-1 REDSHIFT_URI="postgres://user:password@localhost:5439/snowplow" bundle exec ruby load.rb 2016-07-29
```

If the processing fails in the middle it's safe to restart the script from last unfinished day.

Testing showed that loading of a single day of data takes about 1 hour 40 minutes to process.
