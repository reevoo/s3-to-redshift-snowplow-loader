--
-- PostgreSQL database dump
--

-- Dumped from database version 8.0.2
-- Dumped by pg_dump version 9.6.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;
SET row_security = off;

--
-- Name: plpgsql; Type: PROCEDURAL LANGUAGE; Schema: -; Owner: rdsdb
--

CREATE OR REPLACE PROCEDURAL LANGUAGE plpgsql;


ALTER PROCEDURAL LANGUAGE plpgsql OWNER TO rdsdb;

--
-- Name: plpythonu; Type: PROCEDURAL LANGUAGE; Schema: -; Owner: rdsdb
--

CREATE OR REPLACE PROCEDURAL LANGUAGE plpythonu;


ALTER PROCEDURAL LANGUAGE plpythonu OWNER TO rdsdb;

SET search_path = pg_catalog;

--
-- Name: CAST (abstime AS integer); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (abstime AS integer) WITHOUT FUNCTION;


--
-- Name: CAST (bit AS bit varying); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (bit AS bit varying) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (cidr AS inet); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (cidr AS inet) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (inet AS cidr); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (inet AS cidr) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (integer AS abstime); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (integer AS abstime) WITHOUT FUNCTION;


--
-- Name: CAST (integer AS oid); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (integer AS oid) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (integer AS regclass); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (integer AS regclass) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (integer AS regoper); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (integer AS regoper) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (integer AS regoperator); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (integer AS regoperator) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (integer AS regproc); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (integer AS regproc) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (integer AS regprocedure); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (integer AS regprocedure) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (integer AS regtype); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (integer AS regtype) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (integer AS reltime); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (integer AS reltime) WITHOUT FUNCTION;


--
-- Name: CAST (oid AS integer); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (oid AS integer) WITHOUT FUNCTION AS ASSIGNMENT;


--
-- Name: CAST (oid AS regclass); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (oid AS regclass) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (oid AS regoper); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (oid AS regoper) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (oid AS regoperator); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (oid AS regoperator) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (oid AS regproc); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (oid AS regproc) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (oid AS regprocedure); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (oid AS regprocedure) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (oid AS regtype); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (oid AS regtype) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (regclass AS integer); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regclass AS integer) WITHOUT FUNCTION AS ASSIGNMENT;


--
-- Name: CAST (regclass AS oid); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regclass AS oid) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (regoper AS integer); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regoper AS integer) WITHOUT FUNCTION AS ASSIGNMENT;


--
-- Name: CAST (regoper AS oid); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regoper AS oid) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (regoper AS regoperator); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regoper AS regoperator) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (regoperator AS integer); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regoperator AS integer) WITHOUT FUNCTION AS ASSIGNMENT;


--
-- Name: CAST (regoperator AS oid); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regoperator AS oid) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (regoperator AS regoper); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regoperator AS regoper) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (regproc AS integer); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regproc AS integer) WITHOUT FUNCTION AS ASSIGNMENT;


--
-- Name: CAST (regproc AS oid); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regproc AS oid) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (regproc AS regprocedure); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regproc AS regprocedure) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (regprocedure AS integer); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regprocedure AS integer) WITHOUT FUNCTION AS ASSIGNMENT;


--
-- Name: CAST (regprocedure AS oid); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regprocedure AS oid) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (regprocedure AS regproc); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regprocedure AS regproc) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (regtype AS integer); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regtype AS integer) WITHOUT FUNCTION AS ASSIGNMENT;


--
-- Name: CAST (regtype AS oid); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (regtype AS oid) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (reltime AS integer); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (reltime AS integer) WITHOUT FUNCTION;


--
-- Name: CAST (text AS character varying); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (text AS character varying) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (bit varying AS bit); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (bit varying AS bit) WITHOUT FUNCTION AS IMPLICIT;


--
-- Name: CAST (character varying AS text); Type: CAST; Schema: pg_catalog; Owner: 
--

CREATE CAST (character varying AS text) WITHOUT FUNCTION AS IMPLICIT;


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = true;

--
-- Name: __product_dashboard_products_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE __product_dashboard_products_data (
    id bigint DEFAULT "identity"(261484, 0, '1,1'::text) NOT NULL,
    display_identifier bigint,
    display_level bigint,
    display_name character varying(500),
    retailer_name character varying(255),
    trkref character varying(20)
);


ALTER TABLE __product_dashboard_products_data OWNER TO tableau;

--
-- Name: _myreevoo_client_users; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE _myreevoo_client_users (
    user_id character(36) NOT NULL,
    client_id character(36) NOT NULL,
    trkref character varying(20) NOT NULL,
    client_name character varying(255)
);


ALTER TABLE _myreevoo_client_users OWNER TO tableau;

--
-- Name: _product_dashboard_emails_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE _product_dashboard_emails_data (
    trkref character varying(20),
    date date,
    emails_sent bigint,
    product_identifier bigint,
    date_week date,
    date_month date
);


ALTER TABLE _product_dashboard_emails_data OWNER TO tableau;

--
-- Name: _product_dashboard_products_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE _product_dashboard_products_data (
    display_name character varying(500),
    retailer_name character varying(255),
    trkref character varying(20),
    product_identifier bigint
);


ALTER TABLE _product_dashboard_products_data OWNER TO tableau;

--
-- Name: _product_dashboard_reviews_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE _product_dashboard_reviews_data (
    review_id integer,
    date date,
    translation_key character varying(255),
    score integer,
    reviewer_segment character varying(255),
    product_identifier bigint,
    date_week date,
    date_month date
);


ALTER TABLE _product_dashboard_reviews_data OWNER TO tableau;

--
-- Name: active_products; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE active_products (
    product_id bigint
);


ALTER TABLE active_products OWNER TO tableau;

--
-- Name: allconversations; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE allconversations (
    "ask_an_owner_questions|id" integer,
    "ask_an_owner_questions|first_name" character varying(510),
    "ask_an_owner_questions|question" character varying(64000),
    "ask_an_owner_questions|product_group_handle" integer,
    "ask_an_owner_questions|accepted" smallint,
    "ask_an_owner_questions|vetted_at" timestamp without time zone,
    "ask_an_owner_questions|created_at" timestamp without time zone,
    "ask_an_owner_questions|updated_at" timestamp without time zone,
    "ask_an_owner_questions|retailer_id" integer,
    "ask_an_owner_questions|referred" timestamp without time zone,
    "ask_an_owner_questions|message_identifier" character varying(256),
    "ask_an_owner_questions|question_url" character varying(256),
    "ask_an_owner_questions|vetter_id" integer,
    "ask_an_owner_questions|email" character varying(256),
    "ask_an_owner_questions|hashed_email" character varying(256),
    "ask_an_owner_questions|fail_safe_response_sent" timestamp without time zone,
    "ask_an_owner_questions|helpful" integer,
    "ask_an_owner_questions|unhelpful" integer,
    "ask_an_owner_questions|usefulness" double precision,
    "ask_an_owner_questions|retailer_contacted_at" timestamp without time zone,
    "ask_an_owner_questions|retailer_product_series_id" integer,
    "ask_an_owner_questions|permalink_id" character varying(256),
    "ask_an_owner_responses|id" integer,
    "ask_an_owner_responses|ask_an_owner_question_id" integer,
    "ask_an_owner_responses|review_id" integer,
    "ask_an_owner_responses|response" character varying(64000),
    "ask_an_owner_responses|accepted" integer,
    "ask_an_owner_responses|vetted_at" timestamp without time zone,
    "ask_an_owner_responses|created_at" timestamp without time zone,
    "ask_an_owner_responses|updated_at" timestamp without time zone,
    "ask_an_owner_responses|first_name" character varying(510),
    "ask_an_owner_responses|message_identifier" character varying(256),
    "ask_an_owner_responses|job_title" character varying(510),
    "ask_an_owner_responses|vetter_id" integer,
    "ask_an_owner_responses|reviewer_id" integer,
    "ask_an_owner_responses|retailer_id" integer,
    "ask_an_owner_responses|helpful" integer,
    "ask_an_owner_responses|unhelpful" integer,
    "ask_an_owner_responses|usefulness" double precision,
    "ask_an_owner_question_photos|id" integer,
    "ask_an_owner_question_photos|question_id" integer,
    "ask_an_owner_question_photos|photo_uuid" character varying(256),
    "ask_an_owner_question_photos|caption" character varying(256),
    "ask_an_owner_response_photos|id" integer,
    "ask_an_owner_response_photos|response_id" integer,
    "ask_an_owner_response_photos|photo_uuid" character varying(256),
    "ask_an_owner_response_photos|caption" character varying(256),
    "product_group_handles|handle" integer,
    "product_group_handles|product_group_id" integer,
    "product_group_handles|created_at" timestamp without time zone,
    "product_group_handles|updated_at" timestamp without time zone,
    "time difference" double precision,
    "answer period" character varying(256),
    "ask_an_owner_responses|created_at_min" timestamp without time zone,
    "minimum date" double precision
);


ALTER TABLE allconversations OWNER TO tableau;

--
-- Name: analytics_placeholder_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE analytics_placeholder_data (
    placeholder integer
);


ALTER TABLE analytics_placeholder_data OWNER TO tableau;

--
-- Name: client_priority_table_hist; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE client_priority_table_hist (
    "implementation order" double precision,
    "client ga id" character varying(100),
    retailer_id double precision,
    "min date" date,
    last_checked date
);


ALTER TABLE client_priority_table_hist OWNER TO tableau;

--
-- Name: cx_dashboard_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE cx_dashboard_data (
    retailer_trkref character varying(64),
    trkref character varying(20),
    date date,
    date_week date,
    date_month date,
    review_id integer,
    question character varying(50),
    score integer,
    nps_type character varying(64),
    general_comments character varying(2000)
);


ALTER TABLE cx_dashboard_data OWNER TO tableau;

--
-- Name: emails_reviews; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE emails_reviews (
    purchaser_line_item_id integer,
    sent_timestamp timestamp without time zone,
    open_count integer,
    open_timestamp timestamp without time zone,
    click_count integer,
    click_timestamp timestamp without time zone,
    url character varying(255),
    responded_timestamp timestamp without time zone,
    bounce_timestamp timestamp without time zone,
    bounce_type character varying(255),
    sent_via_email_center smallint,
    sent_via_redeye smallint,
    "reviews|id" integer,
    "reviews|status" character varying(255),
    "reviews|created_at" timestamp without time zone,
    "reviews|published" timestamp without time zone,
    bounce_reason character varying(64000)
);


ALTER TABLE emails_reviews OWNER TO tableau;

--
-- Name: ga_metrics; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE ga_metrics (
    retailer_id character varying(128),
    segment_name character varying(256),
    date date,
    uniquepageviews integer,
    newusers integer,
    devicecategory character varying(50),
    timeonpage integer,
    sessions integer,
    users integer,
    pageviews integer,
    pageviewspersession numeric(38,20),
    percentnewsessions numeric(38,20)
);


ALTER TABLE ga_metrics OWNER TO tableau;

--
-- Name: log_cx_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE log_cx_data (
    write_date date,
    write_review_count integer,
    write_trkref_count integer,
    display_date_min date,
    display_date_max date
);


ALTER TABLE log_cx_data OWNER TO tableau;

--
-- Name: log_hist_ga; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE log_hist_ga (
    process_name character varying(50),
    start_time character varying(40),
    end_time character varying(40),
    retailer_id integer,
    order_id integer,
    max_date_recorded character varying(40),
    number_records integer
);


ALTER TABLE log_hist_ga OWNER TO tableau;

--
-- Name: log_inc_ga; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE log_inc_ga (
    process_name character varying(50),
    start_time character varying(50),
    end_time character varying(50),
    number_records integer,
    retailers_recorded integer,
    retailers_not_recorded integer
);


ALTER TABLE log_inc_ga OWNER TO tableau;

--
-- Name: log_processrun; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE log_processrun (
    recordcount bigint,
    startdate timestamp without time zone,
    enddate timestamp without time zone,
    duration integer,
    start_param character varying(64),
    end_param character varying(64),
    process_name character varying(64)
);


ALTER TABLE log_processrun OWNER TO tableau;

--
-- Name: myreevoo_client_users; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE myreevoo_client_users (
    trkref character varying(255),
    client_id character varying(65535),
    client_name character varying(8190),
    user_id character varying(50)
);


ALTER TABLE myreevoo_client_users OWNER TO tableau;

--
-- Name: overview_dashboard_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE overview_dashboard_data (
    trkref character varying(50),
    retailer_trkref character varying(250),
    date_day date,
    date_week date,
    date_month date,
    rendered integer,
    clicked integer,
    didnt_click integer,
    clicked_time_on_site integer,
    didnt_click_time_on_site integer,
    clicked_converted integer,
    didnt_click_converted integer,
    reviews_collected integer,
    emails_sent integer
);


ALTER TABLE overview_dashboard_data OWNER TO tableau;

--
-- Name: overview_dashboard_data_testing; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE overview_dashboard_data_testing (
);


ALTER TABLE overview_dashboard_data_testing OWNER TO tableau;

--
-- Name: product_dashboard_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE product_dashboard_data (
    review_id integer,
    display_name character varying(500),
    display_identifier bigint,
    date date,
    translation_key character varying(255),
    score integer,
    overall_score integer,
    reviewer_segment character varying(255),
    trkref character varying(20),
    retailer_name character varying(255)
);


ALTER TABLE product_dashboard_data OWNER TO tableau;

--
-- Name: product_dashboard_emails_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE product_dashboard_emails_data (
    trkref character varying(20),
    date date,
    emails_sent bigint,
    product_identifier bigint,
    date_week date,
    date_month date
);


ALTER TABLE product_dashboard_emails_data OWNER TO tableau;

--
-- Name: product_dashboard_emails_data_test; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE product_dashboard_emails_data_test (
    trkref character varying(20),
    date date,
    emails_sent bigint,
    product_identifier bigint,
    date_week date,
    date_month date
);


ALTER TABLE product_dashboard_emails_data_test OWNER TO tableau;

--
-- Name: product_dashboard_products_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE product_dashboard_products_data (
    display_name character varying(500),
    trkref character varying(20),
    product_identifier bigint,
    retailer_trkref character varying(500)
);


ALTER TABLE product_dashboard_products_data OWNER TO tableau;

--
-- Name: product_dashboard_products_data_test; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE product_dashboard_products_data_test (
    display_name character varying(500),
    retailer_name character varying(255),
    trkref character varying(20),
    product_identifier bigint
);


ALTER TABLE product_dashboard_products_data_test OWNER TO tableau;

--
-- Name: product_dashboard_reviews_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE product_dashboard_reviews_data (
    review_id integer,
    date date,
    translation_key character varying(255),
    score integer,
    reviewer_segment character varying(255),
    product_identifier bigint,
    date_week date,
    date_month date
);


ALTER TABLE product_dashboard_reviews_data OWNER TO tableau;

--
-- Name: product_dashboard_reviews_data_test; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE product_dashboard_reviews_data_test (
    review_id integer,
    date date,
    translation_key character varying(255),
    score integer,
    reviewer_segment character varying(255),
    product_identifier bigint,
    date_week date,
    date_month date
);


ALTER TABLE product_dashboard_reviews_data_test OWNER TO tableau;

--
-- Name: snowplow_test_table; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE snowplow_test_table (
    countdistinct_domain_sessionid bigint,
    dvce_created_date date,
    event character varying(128),
    os_name character varying(50),
    retailer_id character varying(65535),
    os_family character varying(50),
    os_manufacturer character varying(50),
    dvce_type character varying(50),
    br_name character varying(50),
    br_family character varying(50),
    br_type character varying(50)
);


ALTER TABLE snowplow_test_table OWNER TO tableau;

--
-- Name: stage_incremental_control; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE stage_incremental_control (
    process_name character varying(100) NOT NULL,
    latest_data timestamp without time zone
);


ALTER TABLE stage_incremental_control OWNER TO tableau;

--
-- Name: stg_allconversations; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE stg_allconversations (
    "ask_an_owner_questions|id" integer,
    "ask_an_owner_questions|first_name" character varying(510),
    "ask_an_owner_questions|question" character varying(64000),
    "ask_an_owner_questions|product_group_handle" integer,
    "ask_an_owner_questions|accepted" smallint,
    "ask_an_owner_questions|vetted_at" timestamp without time zone,
    "ask_an_owner_questions|created_at" timestamp without time zone,
    "ask_an_owner_questions|updated_at" timestamp without time zone,
    "ask_an_owner_questions|retailer_id" integer,
    "ask_an_owner_questions|referred" timestamp without time zone,
    "ask_an_owner_questions|message_identifier" character varying(256),
    "ask_an_owner_questions|question_url" character varying(256),
    "ask_an_owner_questions|vetter_id" integer,
    "ask_an_owner_questions|email" character varying(256),
    "ask_an_owner_questions|hashed_email" character varying(256),
    "ask_an_owner_questions|fail_safe_response_sent" timestamp without time zone,
    "ask_an_owner_questions|helpful" integer,
    "ask_an_owner_questions|unhelpful" integer,
    "ask_an_owner_questions|usefulness" double precision,
    "ask_an_owner_questions|retailer_contacted_at" timestamp without time zone,
    "ask_an_owner_questions|retailer_product_series_id" integer,
    "ask_an_owner_questions|permalink_id" character varying(256),
    "ask_an_owner_responses|id" integer,
    "ask_an_owner_responses|ask_an_owner_question_id" integer,
    "ask_an_owner_responses|review_id" integer,
    "ask_an_owner_responses|response" character varying(64000),
    "ask_an_owner_responses|accepted" integer,
    "ask_an_owner_responses|vetted_at" timestamp without time zone,
    "ask_an_owner_responses|created_at" timestamp without time zone,
    "ask_an_owner_responses|updated_at" timestamp without time zone,
    "ask_an_owner_responses|first_name" character varying(510),
    "ask_an_owner_responses|message_identifier" character varying(256),
    "ask_an_owner_responses|job_title" character varying(510),
    "ask_an_owner_responses|vetter_id" integer,
    "ask_an_owner_responses|reviewer_id" integer,
    "ask_an_owner_responses|retailer_id" integer,
    "ask_an_owner_responses|helpful" integer,
    "ask_an_owner_responses|unhelpful" integer,
    "ask_an_owner_responses|usefulness" double precision,
    "ask_an_owner_question_photos|id" integer,
    "ask_an_owner_question_photos|question_id" integer,
    "ask_an_owner_question_photos|photo_uuid" character varying(256),
    "ask_an_owner_question_photos|caption" character varying(256),
    "ask_an_owner_response_photos|id" integer,
    "ask_an_owner_response_photos|response_id" integer,
    "ask_an_owner_response_photos|photo_uuid" character varying(256),
    "ask_an_owner_response_photos|caption" character varying(256),
    "product_group_handles|handle" integer,
    "product_group_handles|product_group_id" integer,
    "product_group_handles|created_at" timestamp without time zone,
    "product_group_handles|updated_at" timestamp without time zone,
    "time difference" double precision,
    "answer period" character varying(256),
    "ask_an_owner_responses|created_at_min" timestamp without time zone,
    "minimum date" double precision
);


ALTER TABLE stg_allconversations OWNER TO tableau;

--
-- Name: stg_emails_reviews; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE stg_emails_reviews (
    purchaser_line_item_id integer,
    sent_timestamp timestamp without time zone,
    open_count integer,
    open_timestamp timestamp without time zone,
    click_count integer,
    click_timestamp timestamp without time zone,
    url character varying(255),
    responded_timestamp timestamp without time zone,
    bounce_timestamp timestamp without time zone,
    bounce_type character varying(255),
    sent_via_email_center smallint,
    sent_via_redeye smallint,
    "reviews|id" integer,
    "reviews|status" character varying(255),
    "reviews|created_at" timestamp without time zone,
    "reviews|published" timestamp without time zone,
    bounce_reason character varying(64000)
);


ALTER TABLE stg_emails_reviews OWNER TO tableau;

--
-- Name: stg_firstrow; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE stg_firstrow (
    stg_firstrow smallint
);


ALTER TABLE stg_firstrow OWNER TO tableau;

--
-- Name: stg_inc_ga_metrics; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE stg_inc_ga_metrics (
    retailer_id character varying(128),
    segment_name character varying(256),
    date date,
    uniquepageviews integer,
    newusers integer,
    devicecategory character varying(50),
    timeonpage integer,
    sessions integer,
    users integer,
    pageviewspersession numeric(38,4),
    percentnewsessions numeric(38,4),
    pageviews integer
);


ALTER TABLE stg_inc_ga_metrics OWNER TO tableau;

--
-- Name: stg_store_ga_profiles; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE stg_store_ga_profiles (
    "client ga id" character varying(100),
    retailer_id character varying(100),
    "min date" date,
    last_checked date
);


ALTER TABLE stg_store_ga_profiles OWNER TO tableau;

--
-- Name: store_ga_profiles; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE store_ga_profiles (
    "client ga id" character varying(100),
    retailer_id character varying(100),
    "min date" date,
    last_checked date
);


ALTER TABLE store_ga_profiles OWNER TO tableau;

--
-- Name: test_join_left; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE test_join_left (
    data_1 integer,
    date date
);


ALTER TABLE test_join_left OWNER TO tableau;

--
-- Name: test_join_right; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE test_join_right (
    data_2 integer,
    date date
);


ALTER TABLE test_join_right OWNER TO tableau;

--
-- Name: test_large_dataset; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE test_large_dataset (
    trkref character(12),
    data1 integer,
    data2 integer,
    data3 integer,
    data4 character(24),
    data5 character(72),
    data6 integer,
    data7 bigint,
    data8 smallint,
    data9 integer,
    data10 integer,
    rowcount integer
);


ALTER TABLE test_large_dataset OWNER TO tableau;

--
-- Name: test_product_dashboard_emails_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE test_product_dashboard_emails_data (
    trkref character varying(20),
    date date,
    emails_sent bigint,
    product_identifier bigint
);


ALTER TABLE test_product_dashboard_emails_data OWNER TO tableau;

--
-- Name: test_product_dashboard_reviews_data; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE test_product_dashboard_reviews_data (
    review_id integer,
    date date,
    translation_key character varying(255),
    score integer,
    overall_score integer,
    reviewer_segment character varying(255),
    product_identifier bigint
);


ALTER TABLE test_product_dashboard_reviews_data OWNER TO tableau;

--
-- Data for Name: __product_dashboard_products_data; Type: TABLE DATA; Schema: public; Owner: tableau
--

COPY __product_dashboard_products_data (id, display_identifier, display_level, display_name, retailer_name, trkref) FROM stdin;
