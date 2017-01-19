package com.reevoo.snowplow.redshift.queries

import com.reevoo.snowplow.RedshiftService


object MarkEventsETLQuery {

  def execute(dateRange: Tuple2[java.sql.Timestamp, java.sql.Timestamp]) = {
    val snowplowDatabase = RedshiftService.snowplowDatabase
    val connection = snowplowDatabase.getConnection
    try {
      snowplowDatabase.executeUpdate(this.query(dateRange), connection)

    } finally {
      connection.close
    }
  }


  private def query(dateRange: Tuple2[java.sql.Timestamp, java.sql.Timestamp]) =
     s"""
      |    INSERT INTO atomic.mark_events_from_s3 (
      |      SELECT
      |        -- App
      |        platform,
      |        -- Date/time
      |        etl_tstamp,
      |        collector_tstamp,
      |        dvce_created_tstamp,
      |        -- Event
      |        event,
      |        event_id,
      |        txn_id,
      |        -- Namespacing and versioning
      |        name_tracker,
      |        v_tracker,
      |        v_collector,
      |        v_etl,
      |        -- User and visit
      |        user_id,
      |        user_ipaddress,
      |        user_fingerprint,
      |        domain_userid,
      |        domain_sessionidx,
      |        network_userid,
      |        -- Location
      |        geo_country,
      |        geo_region,
      |        geo_city,
      |        geo_zipcode,
      |        geo_latitude,
      |        geo_longitude,
      |        geo_region_name,
      |        -- IP lookups
      |        ip_isp,
      |        ip_organization,
      |        ip_domain,
      |        ip_netspeed,
      |        -- Page
      |        page_url,
      |        page_title,
      |        page_referrer,
      |        -- Page URL components
      |        page_urlscheme,
      |        page_urlhost,
      |        page_urlport,
      |        page_urlpath,
      |        page_urlquery,
      |        page_urlfragment,
      |        -- Referrer URL components
      |        refr_urlscheme,
      |        refr_urlhost,
      |        refr_urlport,
      |        refr_urlpath,
      |        refr_urlquery,
      |        refr_urlfragment,
      |        -- Referrer details
      |        refr_medium,
      |        refr_source,
      |        refr_term,
      |        -- Custom structured event
      |        se_category,
      |        se_action,
      |        se_label,
      |        se_property,
      |        se_value,
      |        -- Page ping
      |        pp_xoffset_min,
      |        pp_xoffset_max,
      |        pp_yoffset_min,
      |        pp_yoffset_max,
      |        -- User Agent
      |        useragent,
      |        -- Browser
      |        br_name,
      |        br_family,
      |        br_version,
      |        br_type,
      |        br_renderengine,
      |        br_lang,
      |        br_features_pdf,
      |        br_features_flash,
      |        br_features_java,
      |        br_features_director,
      |        br_features_quicktime,
      |        br_features_realplayer,
      |        br_features_windowsmedia,
      |        br_features_gears,
      |        br_features_silverlight,
      |        br_cookies,
      |        br_colordepth,
      |        br_viewwidth,
      |        br_viewheight,
      |        -- Operating System
      |        os_name,
      |        os_family,
      |        os_manufacturer,
      |        os_timezone,
      |        -- Device/Hardware
      |        dvce_type,
      |        dvce_ismobile,
      |        dvce_screenwidth,
      |        dvce_screenheight,
      |        -- Document
      |        doc_charset,
      |        doc_width,
      |        doc_height,
      |        -- Geolocation
      |        geo_timezone,
      |        -- Click ID
      |        mkt_clickid,
      |        mkt_network,
      |        -- ETL tags
      |        etl_tags,
      |        -- Time event was sent
      |        dvce_sent_tstamp,
      |        -- Referer
      |        refr_domain_userid,
      |        refr_dvce_tstamp,
      |        -- Session ID
      |        domain_sessionid,
      |        -- Derived timestamp
      |        derived_tstamp,
      |        -- Event schema
      |        event_vendor,
      |        event_name,
      |        event_format,
      |        event_version,
      |        -- Event fingerprint
      |        event_fingerprint,
      |        -- True timestamp
      |        true_tstamp,
      |        trkref,
      |        event_type,
      |        reviewable_context,
      |        additional_properties,
      |        content_type,
      |        hit_type,
      |        cta_page_use,
      |        cta_style,
      |        implementation,
      |        TRUE AS s3_copy
      |      FROM (
      |        SELECT
      |          e.*,
      |          -- Common mark event properties
      |          NVL(b.trkref, c.trkref) as trkref,
      |          NVL(b.event_type, c.event_type) as event_type,
      |          NVL(b.reviewable_context, c.reviewable_context) as reviewable_context,
      |          NVL(b.additional_properties, c.additional_properties) as additional_properties,
      |          -- Badge event properties
      |          content_type,
      |          hit_type,
      |          cta_page_use,
      |          cta_style,
      |          implementation,
      |          ROW_NUMBER() OVER (PARTITION BY event_id) as event_number -- select one event at random if the ID is duplicated
      |        FROM atomic.root_events_upload_from_s3 AS e
      |        LEFT JOIN atomic.badge_events_uploaded_from_s3 AS b ON e.event_id = b.root_id AND e.collector_tstamp = b.root_tstamp
      |        LEFT JOIN atomic.conversion_events_uploaded_from_s3 AS c ON e.event_id = c.root_id AND e.collector_tstamp = c.root_tstamp
      |        WHERE e.app_id = 'mark'
      |        AND e.event_id NOT IN (
      |          SELECT me.event_id FROM atomic.mark_events_from_s3 me
      |          WHERE me.collector_tstamp between '${dateRange._1}' AND '${dateRange._2}'
      |        )
      |        AND e.collector_tstamp between '${dateRange._1}' AND '${dateRange._2}'
      |        ORDER BY e.collector_tstamp
      |      ) WHERE event_number = 1
      |    );
    """.stripMargin


}
