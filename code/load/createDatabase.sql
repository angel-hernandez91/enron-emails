-------------------------------
--Create Database and Schemas--
-------------------------------

/*We create the database with ASCII encoding since the processed CSV files are ASCII encoded*/
CREATE DATABASE enron WITH ENCODING 'SQL_ASCII' template=template0;

/*We create the schemas needed for ingesting and analyze the Enron Email Data */
create schema landing_enron;
create schema source_enron;
create schema dev_enron;

/*We create the table DDLs to hold our data */
drop table if exists enron.landing_enron.users cascade;
create table enron.landing_enron.users (
	email_address text not null
	, user_name text
	, is_employee boolean not null
	);

drop table if exists landing_enron.emails cascade;
create table landing_enron.emails (
	message_id text not null constraint emails_pkey primary key
	, sender text not null
	, recipients text
	, copied_recipients text
	, blind_copied_recipients text
	, subject text
	, date timestamp not null
	, sender_name text
	, recipient_names text
	, copied_names text
	, blind_copied_names text
	, body text
	, is_forwarded boolean not null
	, is_chain boolean not null
	, chain_count int not null
	, source_folder text not null
	);
	
drop table if exists source_enron.emails cascade;
create table source_enron.emails (
	message_id text not null constraint emails_pkey primary key
	, sender text not null
	, recipients text
	, copied_recipients text
	, blind_copied_recipients text
	, subject text
	, date timestamp not null
	, sender_name text
	, recipient_names text
	, copied_names text
	, blind_copied_names text
	, body text
	, is_forwarded boolean not null
	, is_chain boolean not null
	, chain_count int not null
	, source_folder text not null
	);

drop table if exists source_enron.users cascade;
create table source_enron.users 
	(email_address text not null constraint users_pkey primary key
	, user_name text
	, is_employee boolean not null
	);