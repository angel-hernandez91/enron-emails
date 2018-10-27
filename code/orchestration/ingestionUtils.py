import os
from datetime import datetime, timedelta
import re
from glob import glob
import email


#check to see if an email is part of chain by looking for the '-----Original Message-----' tag
#addtionally count the number of times the tag appears to get the depth of the chain
def isChain(payload):
	chain_count = 0
	is_chain = False
	check_value = '-----Original Message-----'
	if check_value in payload:
		chain_count = payload.count(check_value)
		is_chain = True
	return chain_count, is_chain

#check to see if an email is forwarded by looking for 'fw' and 'fwd' tags
def isForwarded(body, subject):
	is_forwarded = False
	check_values = ['fw:', 'fwd:']
	for value in check_values:
		if value in body.lower() or value in subject.lower():
			is_forwarded = True
	return is_forwarded


#normalize dates and timezones by converting to UTC and a POSTGRESQL ingestable format
def formatEmailDate(date):
	raw_date_format = email.utils.parsedate_tz(date)
	timestamp = email.utils.mktime_tz(raw_date_format)
	utc_date = datetime(1970, 1, 1) + timedelta(seconds=timestamp)
	return utc_date.__str__()

#get only the names from the x-headers in the e-mail
def parseXHeaders(header):
	if header is not None:
		return re.sub(r'(<.*?>,|<.*?>)', '|', header)[:-1]
	else:
		return header
#get only the numerics from the messageid to create a unique key
def parseMessageId(messageId):
	return re.sub(r'\.|<|>|[a-zA-Z@]', '', messageId)

#check if an employee is an eron employee by looking for 'enron.com' in the email
#this is not ideal, but due the nature of the data this is best we can do
def isEnronEmployee(email):
	employee_flag = False
	if 'enron.com' in email.lower():
		employee_flag = True
	return employee_flag

#search through the maildir path and find all the email files that will then be fed into the emailParser function
def getEmails(searchPath):
	email_file = []
	for root, dirs, files in os.walk(searchPath):
		for directory in dirs:
			curr_dir = glob(os.path.join(root, directory, "*"))
			for file in curr_dir:
				if os.path.isfile(file):
					email_file.append(file)
	return email_file
