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

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = true;

--
-- Name: overview_dashboard_data_testing; Type: TABLE; Schema: public; Owner: tableau
--

CREATE TABLE overview_dashboard_data_testing (
);


ALTER TABLE overview_dashboard_data_testing OWNER TO tableau;

--
-- Data for Name: overview_dashboard_data_testing; Type: TABLE DATA; Schema: public; Owner: tableau
--

COPY overview_dashboard_data_testing (trkref, retailer_trkref, date_day, date_week, date_month, rendered, clicked, didnt_click, clicked_time_on_site, didnt_click_time_on_site, clicked_converted, didnt_click_converted, reviews_collected, emails_sent) FROM stdin;
