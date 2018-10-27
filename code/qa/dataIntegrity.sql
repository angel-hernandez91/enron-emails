/*We check that the data in the Emails table matches exactly with all of the users in the  Users table.
The count should be zero, or else we have a data integrity issue.
*/

select count(*) from (
	select * from (
		select distinct sender 
		from source_enron.emails) as emails
		left join source_enron.users as users
			on emails.sender = users.email_address
		) as a
where email_address is null;