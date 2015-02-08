--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: assignments; Type: TABLE; Schema: public; Owner: meteor; Tablespace: 
--

CREATE TABLE assignments (
    id integer NOT NULL,
    class_id integer NOT NULL,
    name character varying(50),
    value integer NOT NULL
);


ALTER TABLE public.assignments OWNER TO meteor;

--
-- Name: assignments_id_seq; Type: SEQUENCE; Schema: public; Owner: meteor
--

CREATE SEQUENCE assignments_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.assignments_id_seq OWNER TO meteor;

--
-- Name: assignments_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: meteor
--

ALTER SEQUENCE assignments_id_seq OWNED BY assignments.id;


--
-- Name: scores; Type: TABLE; Schema: public; Owner: meteor; Tablespace: 
--

CREATE TABLE scores (
    id integer NOT NULL,
    assignment_id integer NOT NULL,
    student_id integer NOT NULL,
    score integer NOT NULL
);


ALTER TABLE public.scores OWNER TO meteor;

--
-- Name: scores_id_seq; Type: SEQUENCE; Schema: public; Owner: meteor
--

CREATE SEQUENCE scores_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.scores_id_seq OWNER TO meteor;

--
-- Name: scores_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: meteor
--

ALTER SEQUENCE scores_id_seq OWNED BY scores.id;


--
-- Name: students; Type: TABLE; Schema: public; Owner: meteor; Tablespace: 
--

CREATE TABLE students (
    id integer NOT NULL,
    name character varying(50) NOT NULL
);


ALTER TABLE public.students OWNER TO meteor;

--
-- Name: students_id_seq; Type: SEQUENCE; Schema: public; Owner: meteor
--

CREATE SEQUENCE students_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.students_id_seq OWNER TO meteor;

--
-- Name: students_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: meteor
--

ALTER SEQUENCE students_id_seq OWNED BY students.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: meteor
--

ALTER TABLE ONLY assignments ALTER COLUMN id SET DEFAULT nextval('assignments_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: meteor
--

ALTER TABLE ONLY scores ALTER COLUMN id SET DEFAULT nextval('scores_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: meteor
--

ALTER TABLE ONLY students ALTER COLUMN id SET DEFAULT nextval('students_id_seq'::regclass);


--
-- Data for Name: assignments; Type: TABLE DATA; Schema: public; Owner: meteor
--

COPY assignments (id, class_id, name, value) FROM stdin;
1	1	Homework	10
2	1	Test	100
3	2	Art Project	30
4	1	HW 2	10
5	1	HW 3	10
6	1	HW 4	10
\.


--
-- Name: assignments_id_seq; Type: SEQUENCE SET; Schema: public; Owner: meteor
--

SELECT pg_catalog.setval('assignments_id_seq', 6, true);


--
-- Data for Name: scores; Type: TABLE DATA; Schema: public; Owner: meteor
--

COPY scores (id, assignment_id, student_id, score) FROM stdin;
1	1	1	9
2	1	2	8
3	2	1	75
4	2	2	77
5	2	3	50
6	3	1	20
10	4	1	7
11	5	1	8
\.


--
-- Name: scores_id_seq; Type: SEQUENCE SET; Schema: public; Owner: meteor
--

SELECT pg_catalog.setval('scores_id_seq', 11, true);


--
-- Data for Name: students; Type: TABLE DATA; Schema: public; Owner: meteor
--

COPY students (id, name) FROM stdin;
1	John Doe
2	Larry Loe
3	Oklahoma
\.


--
-- Name: students_id_seq; Type: SEQUENCE SET; Schema: public; Owner: meteor
--

SELECT pg_catalog.setval('students_id_seq', 2, true);


--
-- Name: assignments_pkey; Type: CONSTRAINT; Schema: public; Owner: meteor; Tablespace: 
--

ALTER TABLE ONLY assignments
    ADD CONSTRAINT assignments_pkey PRIMARY KEY (id);


--
-- Name: scores_pkey; Type: CONSTRAINT; Schema: public; Owner: meteor; Tablespace: 
--

ALTER TABLE ONLY scores
    ADD CONSTRAINT scores_pkey PRIMARY KEY (id);


--
-- Name: students_pkey; Type: CONSTRAINT; Schema: public; Owner: meteor; Tablespace: 
--

ALTER TABLE ONLY students
    ADD CONSTRAINT students_pkey PRIMARY KEY (id);


--
-- Name: assignments; Type: ACL; Schema: public; Owner: meteor
--

REVOKE ALL ON TABLE assignments FROM PUBLIC;
REVOKE ALL ON TABLE assignments FROM meteor;
GRANT ALL ON TABLE assignments TO meteor;
GRANT ALL ON TABLE assignments TO PUBLIC;


--
-- Name: scores; Type: ACL; Schema: public; Owner: meteor
--

REVOKE ALL ON TABLE scores FROM PUBLIC;
REVOKE ALL ON TABLE scores FROM meteor;
GRANT ALL ON TABLE scores TO meteor;
GRANT ALL ON TABLE scores TO PUBLIC;


--
-- Name: students; Type: ACL; Schema: public; Owner: meteor
--

REVOKE ALL ON TABLE students FROM PUBLIC;
REVOKE ALL ON TABLE students FROM meteor;
GRANT ALL ON TABLE students TO meteor;
GRANT ALL ON TABLE students TO PUBLIC;


--
-- PostgreSQL database dump complete
--

