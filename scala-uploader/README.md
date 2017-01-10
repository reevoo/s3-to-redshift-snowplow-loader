## Development

You need Java 1.7.

In addition sbt and scala, you can install them with the commands below:

```sh
brew update
brew install scala
brew install sbt
```

Whenever you make changes to the source code, you need to regenerate the fat jar by executing the following command:

```sh
sbt assembly
```

This will generate a file "snowplow_historic_event_uploader-assembly-1.0.jar" inside the "./target/scala-2.11" folder.

## Running the uploader

Set the following environment variables in the shell where you will run the uploader:

```sh
export SNOWPLOW_AWS_ACCESS_KEY_ID="whatever"
export SNOWPLOW_AWS_SECRET_ACCESS_KEY="whatever"
export TARGET_REDSHIFT_DB_URL="jdbc:redshift://snowplow-reevoo-redshift-resredshiftcluster-bwgle4yffrt7.c40ocb3ekdkx.eu-west-1.redshift.amazonaws.com:5439/snowplow"
export TARGET_REDSHIFT_DB_USER="username"
export TARGET_REDSHIFT_DB_PASSWORD="password"
```

To run the uploader run the following command (adjust the fromDate and toDate as necessary):

```sh
java -classpath whatever_the_path_to_the_fat_jar/snowplow_historic_event_uploader-assembly-1.0.jar com.reevoo.snowplow.SnowplowToRedshiftHistoricalDataUploader --fromDate 2016-06-10 --toDate 2016-06-10 --overwrite
```

This will upload all the events for the indicated dates into a table named "atomic.mark_events_historic" in the target redshift database.

If you provide "--overwrite" to the command, the table will be truncated before uploading, otherwise the events will be added to whatever other events there were in the table.
