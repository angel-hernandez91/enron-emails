/*We have a total of 71,813 users represented in the Enron Email Data.*/
select count(*) as total_email_address from source_enron.users;

/*We have approximately 30,897 users in this dataset are Enron employees*/
select count(*) as total_enron_employees from source_enron.users where is_employee is true;

/*However, since employees can have more than one e-mail address, this number is overstated 
We attempt to find the eron employees with more than one e-mail address:
	This does have serveral issues:
		A mailing group can be counted as an enron employee since we assume enron emoployees have the extension 'enron.com'
		Employee names are not unique, i.e. we have could two distinct people named Matt Hull
		The mapping of Email Addresses to Names was done under the assumption that the From, To, CC, BCC fields matched exactly with the corresponding X-Headers, but this was not always the case
		Due to the nature of the data gather, we have mutliple e-mail address duplicates where one e-mail was tied to different names. Since it was not possible to find the correct name without implementing a fuzzy matching step, we simiply de-dupped the email field by picking a random name
*/

with duplicated_users as (		
/* First query finds the duplicated users by user_name. We do have situations where
where 1 employee clearly has two different e-mail address (e.g., Reverse First/Last names)
*/
	select * from source_enron.users where user_name in (
		select user_name from (
			select user_name, count(*) from source_enron.users
			where is_employee is true
			group by user_name
			having count(*) >1) as user_dups
		)
	order by user_name)
/*We count distinct users*/
select count(distinct user_name) from duplicated_users where is_employee is true;

/*We subtract to get better estimate 30,897 - 1555 = 29,342*/

/*We have a total of 517,401 emails sent in the Enron Emal Data*/
select count(*) as total_emails_sent from source_enron.emails;

/*We create a view for easy access to the count of emails per sender.*/
create view dev_enron.emails_per_sender as (
	select sender, count(*) as emails_per_sender from source_enron.emails
	group by sender
	order by emails_per_sender desc
);

/* We know this is reasonable since someone like Vince Kaminski (Managing Director) is at the top. */
select * from dev_enron.emails_per_sender limit 100;


/* We find that 76,138 emails are chains. This number could be understated this is not an embedded MIME tag.
We found this number by parsing the body for the '--Original Message-- tag */
select count(*) as email_chains from source_enron.emails 
where is_chain is true; 

/* For those emails that are chains we have a total depth of 140,831. See above for potential understantment */
select sum(chain_count) as all_chains from source_enron.emails 
where is_chain is true;

/*We have 49,576 emails forwards. This number could be understated since forwarded in not embedded
in the MIME data. We found this number by parsing the Subject and Body for the FW or Fwd tag. */
select count(*) as forwarded_emails from source_enron.emails
where is_forwarded is true;
/*We end up with the follow ratios:
	49,576/517,401 is the ratio of Forwards to total e-mails
	49,576/517,358 is the ratio of fowards to inital e-mails
*/


/*We find the ratio of all e-mails sent for Eron employees vs all Emails present in the data.*/
create view dev_enron.total_emails_sent as (
	select count(*) as eron_employee_emails_sent
		, max(total_emails) as total_emails 
	from (select e.*
		, u.is_employee
		, count(*) over() as total_emails 
	      from source_enron.emails as e
	      left join source_enron.users as u
			on e.sender = u.email_address
	) as joined 
where is_employee is true);

/*We see that 83% of emails cam from Enron Employees. */
select eron_employee_emails_sent/total_emails::float * 100.0 as percent_of_internal_emails 
from dev_enron.total_emails_sent;
