require "rubygems"
require "bundler/setup"
require "dotenv"
require "pry"
require 'aws-sdk'

Bundler.require(:default)

$LOAD_PATH << File.expand_path("../", __FILE__)

Dotenv.load ".env"

START_DATE = Date.parse('2016-07-29')
S3 = Aws::S3::Client.new
REDSHIFT = PG::Connection.new(ENV['REDSHIFT_URI'])

def copy_to_redshift(table_name, bucket_path)
  copy_sql = <<-eos
    COPY #{table_name}
    FROM 's3://snowplow-reevoo-unload/#{bucket_path}'
    WITH CREDENTIALS 'aws_access_key_id=#{ENV['AWS_ACCESS_KEY_ID']};aws_secret_access_key=#{ENV['AWS_SECRET_ACCESS_KEY']}'
    GZIP REMOVEQUOTES ESCAPE;
  eos
  REDSHIFT.exec(copy_sql)
end

def retrieve_directories(main_dir, continuation_token = nil)
  S3.list_objects(
    bucket: 'snowplow-reevoo-unload',
    prefix: "#{main_dir}/",
    delimiter: '/',
    continuation_token: continuation_token
  )
end

def all_s3_prefixes(main_dir)
  prefixes = []
  begin
    dir_response = retrieve_directories(main_dir, dir_response && dir_response.next_continuation_token)
    prefixes.concat(dir_response.common_prefixes.map(&:prefix))
  end while dir_response.is_truncated
  prefixes
end

def filtered_s3_prefixes(main_dir, date_since)
  all_s3_prefixes(main_dir).select do |prefix|
    prefix =~ /events\/(\d{4}-\d{2}-\d{2}.+)\// && Date.parse($1) >= date_since
  end
end


filtered_s3_prefixes('events', START_DATE).each do |prefix|
  puts "COPY #{prefix}"
  copy_to_redshift('landing.copy_events', prefix)
end







