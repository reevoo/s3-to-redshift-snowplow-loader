require "rubygems"
require "bundler/setup"
require "aws-sdk"
require "pg"
# require "dotenv"
# require "pry"

# Dotenv.load ".env"

START_DATE      = ARGV[0] ? Date.parse(ARGV[0]) : Date.today - 30
END_DATE        = ARGV[1] ? Date.parse(ARGV[1]) : Date.today

TABLES          = %w(events com_reevoo_badge_event_1 com_reevoo_conversion_event_1)
S3              = Aws::S3::Client.new
REDSHIFT        = PG::Connection.new(ENV['REDSHIFT_URI'])
TIME_SLICE_STEP = Rational(1, 144)


def copy_to_redshift(table_name, bucket_path)
  copy_sql = <<-eos
    COPY #{table_name}
    FROM 's3://snowplow-reevoo-unload/#{bucket_path}'
    WITH CREDENTIALS 'aws_access_key_id=#{ENV['AWS_ACCESS_KEY_ID']};aws_secret_access_key=#{ENV['AWS_SECRET_ACCESS_KEY']}'
    GZIP REMOVEQUOTES ESCAPE;
  eos
  REDSHIFT.exec(copy_sql)
end

def mark_events_etl(from, to)
  etl_sql = <<-eos
    INSERT INTO atomic.mark_events (
      SELECT
        -- App
        platform,
        -- Date/time
        etl_tstamp,
        collector_tstamp,
        dvce_created_tstamp,
        -- Event
        event,
        event_id,
        txn_id,
        -- Namespacing and versioning
        name_tracker,
        v_tracker,
        v_collector,
        v_etl,
        -- User and visit
        user_id,
        user_ipaddress,
        user_fingerprint,
        domain_userid,
        domain_sessionidx,
        network_userid,
        -- Location
        geo_country,
        geo_region,
        geo_city,
        geo_zipcode,
        geo_latitude,
        geo_longitude,
        geo_region_name,
        -- IP lookups
        ip_isp,
        ip_organization,
        ip_domain,
        ip_netspeed,
        -- Page
        page_url,
        page_title,
        page_referrer,
        -- Page URL components
        page_urlscheme,
        page_urlhost,
        page_urlport,
        page_urlpath,
        page_urlquery,
        page_urlfragment,
        -- Referrer URL components
        refr_urlscheme,
        refr_urlhost,
        refr_urlport,
        refr_urlpath,
        refr_urlquery,
        refr_urlfragment,
        -- Referrer details
        refr_medium,
        refr_source,
        refr_term,
        -- Custom structured event
        se_category,
        se_action,
        se_label,
        se_property,
        se_value,
        -- Page ping
        pp_xoffset_min,
        pp_xoffset_max,
        pp_yoffset_min,
        pp_yoffset_max,
        -- User Agent
        useragent,
        -- Browser
        br_name,
        br_family,
        br_version,
        br_type,
        br_renderengine,
        br_lang,
        br_features_pdf,
        br_features_flash,
        br_features_java,
        br_features_director,
        br_features_quicktime,
        br_features_realplayer,
        br_features_windowsmedia,
        br_features_gears,
        br_features_silverlight,
        br_cookies,
        br_colordepth,
        br_viewwidth,
        br_viewheight,
        -- Operating System
        os_name,
        os_family,
        os_manufacturer,
        os_timezone,
        -- Device/Hardware
        dvce_type,
        dvce_ismobile,
        dvce_screenwidth,
        dvce_screenheight,
        -- Document
        doc_charset,
        doc_width,
        doc_height,
        -- Geolocation
        geo_timezone,
        -- Click ID
        mkt_clickid,
        mkt_network,
        -- ETL tags
        etl_tags,
        -- Time event was sent
        dvce_sent_tstamp,
        -- Referer
        refr_domain_userid,
        refr_dvce_tstamp,
        -- Session ID
        domain_sessionid,
        -- Derived timestamp
        derived_tstamp,
        -- Event schema
        event_vendor,
        event_name,
        event_format,
        event_version,
        -- Event fingerprint
        event_fingerprint,
        -- True timestamp
        true_tstamp,

        trkref,
        event_type,
        reviewable_context,
        additional_properties,
        content_type,
        hit_type,
        cta_page_use,
        cta_style,
        implementation,

        TRUE AS s3_copy

      FROM (
        SELECT
          e.*,

          -- Common mark event properties
          NVL(b.trkref, c.trkref) as trkref,
          NVL(b.event_type, c.event_type) as event_type,
          NVL(b.reviewable_context, c.reviewable_context) as reviewable_context,
          NVL(b.additional_properties, c.additional_properties) as additional_properties,

          -- Badge event properties
          content_type,
          hit_type,
          cta_page_use,
          cta_style,
          implementation,

          ROW_NUMBER() OVER (PARTITION BY event_id) as event_number -- select one event at random if the ID is duplicated

        FROM landing.copy_events AS e
        LEFT JOIN landing.copy_com_reevoo_badge_event_1 AS b ON e.event_id = b.root_id AND e.collector_tstamp = b.root_tstamp
        LEFT JOIN landing.copy_com_reevoo_conversion_event_1 AS c ON e.event_id = c.root_id AND e.collector_tstamp = c.root_tstamp
        WHERE e.app_id = 'mark'
        AND e.event_id NOT IN (
          SELECT me.event_id FROM atomic.mark_events me
          WHERE me.collector_tstamp >= '#{from}' AND me.collector_tstamp < '#{to}'
        )
        AND e.collector_tstamp >= '#{from}' AND e.collector_tstamp < '#{to}'
        ORDER BY e.collector_tstamp

      ) WHERE event_number = 1
    );
  eos
  REDSHIFT.exec(etl_sql)
end

def list_unloaded_items(main_dir, continuation_token = nil)
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
    dir_response = list_unloaded_items(main_dir, dir_response && dir_response.next_continuation_token)
    prefixes.concat(dir_response.common_prefixes.map(&:prefix))
  end while dir_response.is_truncated
  prefixes
end

def s3_prefixes_for_day(main_dir, date)
  all_s3_prefixes(main_dir).select do |prefix|
    prefix =~ /.+\/(\d{4}-\d{2}-\d{2}.+)\// && Date.parse($1) == date
  end
end

def for_event_time_slices
  min, max = REDSHIFT.exec("SELECT min(collector_tstamp), max(collector_tstamp) FROM landing.copy_events;").values[0]
  min = DateTime.parse(min)
  max = DateTime.parse(max)
  slice_min = min
  while slice_min <= max
    yield(slice_min, slice_min + TIME_SLICE_STEP)
    slice_min += TIME_SLICE_STEP
  end
end

def for_dates_between(from, to)
  date = from
  while date <= to
    yield(date)
    date += 1
  end
end


for_dates_between(START_DATE, END_DATE) do |date|
  puts "PROCESSING EVENTS FOR #{date}"

  TABLES.each do |table|
    copy_table = "landing.copy_#{table}"

    puts "TRUNCATE #{copy_table}"
    REDSHIFT.exec("TRUNCATE #{copy_table}")

    s3_prefixes_for_day(table, date).each do |prefix|
      puts "COPY #{prefix}"
      copy_to_redshift(copy_table, prefix)
    end

    puts "ANALYZE #{copy_table}"
    REDSHIFT.exec("ANALYZE #{copy_table}")
  end

  for_event_time_slices do |slice_from, slice_to|
    puts "RUNNING ETL from #{slice_from} to #{slice_to}"
    mark_events_etl(slice_from, slice_to)
  end
end

