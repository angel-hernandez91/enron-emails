-------------------------------------------
--Load the Data from Processed CSV files --
-------------------------------------------
/*We insert the data into the landing tables from our CSV files.*/
copy enron.landing_enron.users 
from '/home/azimov/Documents/code/cmb/output/enron_email_user.csv'
with (
	format csv,
	header False,
	null ''
);

copy enron.landing_enron.emails 
from '/home/azimov/Documents/code/cmb/output/enron_emails.csv'
with (
	format csv,
	header False,
	null ''
);

/*If the landing import does not break from failing the DDL constraints, then we push the data to source.
 Note that we do have to process the User data. Since the Email to Name mappings have issues (i.e., X-Header Fields
 can have different names for each e-mail. Lastly, since an email can only correpsond to one user, we select a random
 user in the event more than one user for an email exists
*/

truncate source_enron.users;
with distinct_null_users as (
	select distinct * from landing_enron.users where user_name is null
	),
distinct_non_null_users as (
	select distinct email_address, user_name, is_employee from (
		select * from 
			(select *, max(user_name) over(partition by email_address) as rand_user
			from landing_enron.users) as rand 
			where user_name = rand_user
		) as final
	),
	
insert_users as (
	select * from distinct_null_users where email_address not in
		(select email_address from distinct_non_null_users)
		)

insert into source_enron.users (
	select * from insert_users union
	select * from distinct_non_null_users
	);

/*No transformations needed for the Email table. So we load as is.*/
truncate source_enron.emails;
insert into source_enron.emails (select * from landing_enron.emails);