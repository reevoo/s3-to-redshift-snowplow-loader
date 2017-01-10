package com.reevoo.snowplow

import java.util.Properties
import java.sql.DriverManager

class RedshiftService(val historicEventsTableName: String) {

  private final val DbUrl = sys.env("TARGET_REDSHIFT_DB_URL")
  private final val DriverClass = "com.amazon.redshift.jdbc41.Driver"
  private final val DBConnectionProperties = {
    val props = new Properties()
    props.setProperty("driver", DriverClass)
    props.setProperty("user", sys.env("TARGET_REDSHIFT_DB_USER"))
    props.setProperty("password", sys.env("TARGET_REDSHIFT_DB_PASSWORD"))
    props
  }

  private final val TemporalRootEventsTableName = "atomic.events_temp"
  private final val TemporalBadgeEventsTableName = "atomic.badge_temp"
  private final val TemporalConversionEventsTableName = "atomic.conversion_temp"


  def uploadEvents(folderDiscriminator: String) = {
    createOrTruncateEventsTempTable
    createOrTruncateBadgeTempTable
    createOrTruncateConversionTempTable
    uploadUnaggregatedEventsToTempTables(folderDiscriminator)
    aggregateEventsIntoMarkEventsHistoricTable
  }


  def createOrTruncateAggregatedEventsTable(overwrite: Boolean) = {
    val connection = getConnection
    val statement = connection.createStatement()

    statement.executeUpdate(
      s"CREATE TABLE IF NOT EXISTS ${historicEventsTableName} (" +
        "dvce_created_tstamp   TIMESTAMP," +
        "event_id              VARCHAR(36)," +
        "network_userid        VARCHAR(38)," +
        "domain_sessionid      VARCHAR(36)," +
        "trkref                VARCHAR(20)," +
        "event_type            VARCHAR(20)," +
        "reviewable_context    VARCHAR(4096)," +
        "additional_properties VARCHAR(4096)," +
        "content_type          VARCHAR(19)," +
        "hit_type              VARCHAR(14)," +
        "cta_page_use          VARCHAR(17)," +
        "cta_style             VARCHAR(40)," +
        "implementation        VARCHAR(14)) " +
        "DISTSTYLE KEY " +
        "DISTKEY (event_id) " +
        "INTERLEAVED SORTKEY (trkref, event_type, dvce_created_tstamp); ")

    if (overwrite) statement.executeUpdate(s"TRUNCATE TABLE ${historicEventsTableName}")

    connection.close()
  }

  private def getConnection = {
    Class.forName(DriverClass)
    DriverManager.getConnection(DbUrl, DBConnectionProperties)
  }

  private def createOrTruncateEventsTempTable = {
    val connection = getConnection
    val statement = connection.createStatement()

    statement.executeUpdate(
      s"CREATE TABLE IF NOT EXISTS ${TemporalRootEventsTableName} ( " +
        "app_id varchar(255) encode text255, " +
        "platform varchar(1), " +
        "etl_tstamp varchar(1), " +
        "collector_tstamp timestamp, " +
        "dvce_created_tstamp timestamp, " +
        "event varchar(128) encode text255, " +
        "event_id char(36), " +
        "txn_id varchar(1), " +
        "name_tracker varchar(1), " +
        "v_tracker varchar(1), " +
        "v_collector varchar(1), " +
        "v_etl varchar(1), " +
        "user_id varchar(1), " +
        "user_ipaddress varchar(1), " +
        "user_fingerprint varchar(1), " +
        "domain_userid varchar(1), " +
        "domain_sessionidx varchar(1), " +
        "network_userid varchar(38), " +
        "geo_country varchar(1), " +
        "geo_region varchar(1), " +
        "geo_city varchar(1), " +
        "geo_zipcode varchar(1), " +
        "geo_latitude varchar(1), " +
        "geo_longitude varchar(1), " +
        "geo_region_name varchar(1), " +
        "ip_isp varchar(1), " +
        "ip_organization varchar(1), " +
        "ip_domain varchar(1), " +
        "ip_netspeed varchar(1), " +
        "page_url varchar(1), " +
        "page_title varchar(1), " +
        "page_referrer varchar(1), " +
        "page_urlscheme varchar(1), " +
        "page_urlhost varchar(1), " +
        "page_urlport varchar(1), " +
        "page_urlpath varchar(1), " +
        "page_urlquery varchar(1), " +
        "page_urlfragment varchar(1), " +
        "refr_urlscheme varchar(1), " +
        "refr_urlhost varchar(1), " +
        "refr_urlport varchar(1), " +
        "refr_urlpath varchar(1), " +
        "refr_urlquery varchar(1), " +
        "refr_urlfragment varchar(1), " +
        "refr_medium varchar(1), " +
        "refr_source varchar(1), " +
        "refr_term varchar(1), " +
        "mkt_medium varchar(1), " +
        "mkt_source varchar(1), " +
        "mkt_term varchar(1), " +
        "mkt_content varchar(1), " +
        "mkt_campaign varchar(1), " +
        "se_category varchar(1), " +
        "se_action varchar(1), " +
        "se_label varchar(1), " +
        "se_property varchar(1), " +
        "se_value varchar(1), " +
        "tr_orderid varchar(1), " +
        "tr_affiliation varchar(1), " +
        "tr_total varchar(1), " +
        "tr_tax varchar(1), " +
        "tr_shipping varchar(1), " +
        "tr_city varchar(1), " +
        "tr_state varchar(1), " +
        "tr_country varchar(1), " +
        "ti_orderid varchar(1), " +
        "ti_sku varchar(1), " +
        "ti_name varchar(1), " +
        "ti_category varchar(1), " +
        "ti_price varchar(1), " +
        "ti_quantity varchar(1), " +
        "pp_xoffset_min varchar(1), " +
        "pp_xoffset_max varchar(1), " +
        "pp_yoffset_min varchar(1), " +
        "pp_yoffset_max varchar(1), " +
        "useragent varchar(1), " +
        "br_name varchar(1), " +
        "br_family varchar(1), " +
        "br_version varchar(1), " +
        "br_type varchar(1), " +
        "br_renderengine varchar(1), " +
        "br_lang varchar(1), " +
        "br_features_pdf varchar(1), " +
        "br_features_flash varchar(1), " +
        "br_features_java varchar(1), " +
        "br_features_director varchar(1), " +
        "br_features_quicktime varchar(1), " +
        "br_features_realplayer varchar(1), " +
        "br_features_windowsmedia varchar(1), " +
        "br_features_gears varchar(1), " +
        "br_features_silverlight varchar(1), " +
        "br_cookies varchar(1), " +
        "br_colordepth varchar(1), " +
        "br_viewwidth varchar(1), " +
        "br_viewheight varchar(1), " +
        "os_name varchar(1), " +
        "os_family varchar(1), " +
        "os_manufacturer varchar(1), " +
        "os_timezone varchar(1), " +
        "dvce_type varchar(1), " +
        "dvce_ismobile varchar(1), " +
        "dvce_screenwidth varchar(1), " +
        "dvce_screenheight varchar(1), " +
        "doc_charset varchar(1), " +
        "doc_width varchar(1), " +
        "doc_height varchar(1), " +
        "tr_currency varchar(1), " +
        "tr_total_base varchar(1), " +
        "tr_tax_base varchar(1), " +
        "tr_shipping_base varchar(1), " +
        "ti_currency varchar(1), " +
        "ti_price_base varchar(1), " +
        "base_currency varchar(1), " +
        "geo_timezone varchar(1), " +
        "mkt_clickid varchar(1), " +
        "mkt_network varchar(1), " +
        "etl_tags varchar(1), " +
        "dvce_sent_tstamp timestamp, " +
        "refr_domain_userid varchar(1), " +
        "refr_dvce_tstamp varchar(1), " +
        "domain_sessionid char(36) encode raw, " +
        "derived_tstamp varchar(1), " +
        "event_vendor varchar(1), " +
        "event_name varchar(1), " +
        "event_format varchar(1), " +
        "event_version varchar(1), " +
        "event_fingerprint varchar(1), " +
        "true_tstamp varchar(1) " +
        "); ")

    statement.executeUpdate(s"TRUNCATE TABLE ${TemporalRootEventsTableName}")

    connection.close()
  }

  private def createOrTruncateBadgeTempTable = {
    val connection = getConnection
    val statement = connection.createStatement()

    statement.executeUpdate(
      s"CREATE TABLE IF NOT EXISTS ${TemporalBadgeEventsTableName} ( " +
        "schema_vendor         varchar(1), " +
        "schema_name           varchar(1), " +
        "schema_format         varchar(1), " +
        "schema_version        varchar(1), " +
        "root_id               CHAR(36)      ENCODE RAW, " +
        "root_tstamp           TIMESTAMP     ENCODE LZO, " +
        "ref_root              varchar(1), " +
        "ref_tree              varchar(1), " +
        "ref_parent            varchar(1), " +
        "content_type          VARCHAR(19)   ENCODE LZO, " +
        "cta_page_use          VARCHAR(17)   ENCODE LZO, " +
        "event_type            VARCHAR(8)    ENCODE LZO, " +
        "implementation        VARCHAR(14)   ENCODE LZO, " +
        "trkref                VARCHAR(20)   ENCODE LZO, " +
        "additional_properties VARCHAR(4096) ENCODE LZO, " +
        "cta_style             VARCHAR(40)   ENCODE LZO, " +
        "hit_type              VARCHAR(14)   ENCODE LZO, " +
        "reviewable_context    VARCHAR(4096) ENCODE LZO " +
        "); ")

    statement.executeUpdate(s"TRUNCATE TABLE ${TemporalBadgeEventsTableName}")

    connection.close()
  }

  private def createOrTruncateConversionTempTable = {
    val connection = getConnection
    val statement = connection.createStatement()

    statement.executeUpdate(
      s"CREATE TABLE IF NOT EXISTS ${TemporalConversionEventsTableName} ( " +
        "schema_vendor         varchar(1), " +
        "schema_name           varchar(1), " +
        "schema_format         varchar(1), " +
        "schema_version        varchar(1), " +
        "root_id               CHAR(36)      ENCODE RAW, " +
        "root_tstamp           TIMESTAMP     ENCODE LZO, " +
        "ref_root              varchar(1), " +
        "ref_tree              varchar(1), " +
        "ref_parent            varchar(1), " +
        "event_type            VARCHAR(17)   ENCODE LZO, " +
        "trkref                VARCHAR(20)   ENCODE LZO, " +
        "additional_properties VARCHAR(4096) ENCODE LZO, " +
        "reviewable_context    VARCHAR(4096) ENCODE LZO " +
        "); ")

    statement.executeUpdate(s"TRUNCATE TABLE ${TemporalConversionEventsTableName}")

    connection.close()
  }

  private def uploadUnaggregatedEventsToTempTables(folderDiscriminator: String) = {
    val selector = new AmazonS3Service
    selector.getRootEventFolders(folderDiscriminator).foreach {
      endpoint => uploadToTable(TemporalRootEventsTableName, endpoint)
    }
    selector.getBadgeEventFolders(folderDiscriminator).foreach {
      endpoint => uploadToTable(TemporalBadgeEventsTableName, endpoint)
    }
    selector.getConversionEventFolders(folderDiscriminator).foreach {
      endpoint => uploadToTable(TemporalConversionEventsTableName, endpoint)
    }
  }

  private def uploadToTable(tableName: String, s3Endpoint: String) = {
    println(s"Copying to table ${tableName} from endpoint ${s3Endpoint}")
    val connection = getConnection
    val statement = connection.createStatement()

    statement.executeUpdate(
      s"COPY ${tableName} FROM '${s3Endpoint}' " +
        "WITH CREDENTIALS " +
        s"'aws_access_key_id=${sys.env("SNOWPLOW_AWS_ACCESS_KEY_ID")};" +
        s"aws_secret_access_key=${sys.env("SNOWPLOW_AWS_SECRET_ACCESS_KEY")}' " +
        "GZIP REMOVEQUOTES ESCAPE TRUNCATECOLUMNS DATEFORMAT 'auto' MAXERROR 100;")

    statement.close()
  }

  private def aggregateEventsIntoMarkEventsHistoricTable = {
    println(s"Aggregating events into table ${historicEventsTableName}")

    val connection = getConnection
    val statement = connection.createStatement()

    statement.executeUpdate(
      s"INSERT INTO ${historicEventsTableName} (" +
        "SELECT " +
        "dvce_created_tstamp, " +
        "event_id, " +
        "network_userid, " +
        "domain_sessionid, " +
        "trkref, " +
        "event_type, " +
        "reviewable_context, " +
        "additional_properties, " +
        "content_type, " +
        "hit_type, " +
        "cta_page_use, " +
        "cta_style, " +
        "implementation " +
        "FROM ( " +
        "SELECT " +
        "e.*, " +
        "NVL(b.trkref, c.trkref) as trkref, " +
        "NVL(b.event_type, c.event_type) as event_type, " +
        "NVL(b.reviewable_context, c.reviewable_context) as reviewable_context, " +
        "NVL(b.additional_properties, c.additional_properties) as additional_properties, " +
        "content_type, " +
        "hit_type, " +
        "cta_page_use, " +
        "cta_style, " +
        "implementation, " +
        "ROW_NUMBER() OVER (PARTITION BY event_id) as event_number " +
        s" FROM ${TemporalRootEventsTableName} AS e " +
        s" LEFT JOIN ${TemporalBadgeEventsTableName} AS b ON e.event_id = b.root_id " +
        s" LEFT JOIN ${TemporalConversionEventsTableName} AS c ON e.event_id = c.root_id " +
        " WHERE e.app_id = 'mark' " +
        ") WHERE event_number = 1 and trkref IS NOT NULL" +
        ")")

    statement.executeUpdate(s"TRUNCATE TABLE ${TemporalRootEventsTableName}")
    statement.executeUpdate(s"TRUNCATE TABLE ${TemporalBadgeEventsTableName}")
    statement.executeUpdate(s"TRUNCATE TABLE ${TemporalConversionEventsTableName}")

    connection.close()
  }

}
