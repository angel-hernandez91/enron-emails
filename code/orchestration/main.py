import csv
#import email
from itertools import izip
import multiprocessing
from contextlib import contextmanager

#import helper files
from ingestionUtils import *
from fetchAndUnzipData import *

#Create a mapping of Email to Name using the corresponding x-headers
def mapEmailUser(emails, usernames, emailUserMap):
	if emails is not None:
		emails = emails.split(',')
	else:
		emails = ['']

	if usernames is not None:
		usernames = usernames.split('|')
	else:
		usernames = ['']
	
	#Assume that Email Address and x-header names correpsond to one another in order
	for email, user in izip(emails, usernames):
		#if no email is present then we skip
		if email == '':
			pass
		else:
			#Reduce number of duplications
			if email not in emailUserMap:
				employee_flag = isEnronEmployee(email)
				emailUserMap[email.strip()] = [user.strip(), employee_flag]
	return emailUserMap

#We use Python Email Parser to parse the email data
def parseEmail(emailFile):
	with open(emailFile) as mail:
		data = email.message_from_file(mail)
		#parsed objects
		message_id = parseMessageId(data['message-id'])
		
		sender = data['from']
		recipients = data['to']	
		copied_recipients = data['cc']
		blind_copied_recipients = data['bcc']
		
		subject = data['subject']
		date = formatEmailDate(data['date'])

		sender_name = parseXHeaders(data['x-from'])
		recipient_names = parseXHeaders(data['x-to'])
		copied_names = parseXHeaders(data['x-cc'])
		blind_copied_names = parseXHeaders(data['x-bcc'])
		
		body = data.get_payload(decode=True)
		
		is_forwarded = isForwarded(body, subject)
		chain_count, is_chain = isChain(body)
		source = emailFile
		#Store parsed data
		parsedData = [message_id
			, sender
			, recipients
			, copied_recipients
			, blind_copied_recipients
			, subject
			, date
			, sender_name
			, recipient_names
			, copied_names
			, blind_copied_names
			, body
			, is_forwarded
			, is_chain
			, chain_count
			, source]

		#save user mappings
		raw_email_list = [sender, recipients, copied_recipients, blind_copied_recipients]
		raw_user_list = [sender_name, recipient_names, copied_names, blind_copied_names]
		emailMap = {}
		for e, u in izip(raw_email_list, raw_user_list):
			mapEmailUser(e, u, emailMap)
	#Return the Parsed Data and the Email to Name mappings
	return parsedData, emailMap

#Get a list of all of the emails in the maildir
allEmails = getEmails(sourceDir)
fileCount = len(allEmails)

#List for writing out to CSV
parsedEmailTable = []
userTable = []


#We use multiprocessing to speed up the parsing process
coreCount = multiprocessing.cpu_count()

#neat trick for handling fucntions with multiple arguments, not needed, but decided to keep around to avoid refactoring
@contextmanager
def poolcontext(*args, **kwargs):
	pool = multiprocessing.Pool(*args, **kwargs)
	yield pool
	pool.terminate()

#set the optimal batch size for each processer to process
def setChunksize(file_count, core_count):
	chunksize = int(round(file_count/core_count))
	if chunksize < 1:
		chunksize = 1
	return chunksize

chunksize = setChunksize(fileCount, coreCount)

#runs the processing using all avalilable cores 
if __name__ == "__main__":
	with poolcontext(processes=coreCount) as pool:
		results = pool.map(parseEmail, allEmails, chunksize=chunksize)


#Write out the Email and User data out to CSV file for ingesting into our POSTGRESQL database
outEmails = '{}enron_emails.csv'.format(outPath)
outUsers = '{}enron_email_user.csv'.format(outPath)

#we transform the tuples in the results variables 
#and we also transform the email dictionary to a list for storing into a csv
for result in results:
	parsedEmailTable.append(result[0])
	for key, value in result[1].items():
		userTable.append([key, value[0], value[1]])

with open(outEmails, 'wb') as out_file:
	writer = csv.writer(out_file)
	writer.writerows(parsedEmailTable)


with open(outUsers, 'wb') as out_file:
	writer = csv.writer(out_file)
	writer.writerows(userTable)