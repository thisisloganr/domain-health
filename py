import pandas as pd
import requests
import os
import xlsxwriter
from datetime import datetime, timedelta, date
import advertools as adv
from advertools import serp_goog
from advertools import sitemap_to_df
from advertools import robotstxt_to_df
from vega_datasets import data
import numpy as np
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import email, smtplib, ssl, socket
from email import encoders
from email.mime.base import MIMEBase
import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials

###Setup
# Read Sheets, create initial dataFrame
###Replace the Google Sheets URL when you make your copy
fp = "https://docs.google.com/spreadsheets/d/1jAuoEFmslQRUijrPA3VLcVLEjCXXByQzlpueq-FTqHs/export?format=csv&gid=0"
df = pd.read_csv(fp)
###Setup

################################################################################

###Multi-Use Functions
# Get last modified date/time
def last_mod(x):
	try:
		headers = {'User-Agent': 'domain-health'}
		r = requests.head(x, headers=headers)
		url_time = r.headers['last-modified']
		return url_time
	except:
		return "No Last Mod"

# Get status code
def url_access(x):
	headers = {'User-Agent': 'domain-health'}
	try:
		return requests.head(x, headers=headers).status_code
	except:
		return "Unavailable"
###Multi-Use Functions

################################################################################

###Canonical Domain Check
# Remove protocol, subdomain, trailing slash
def clean(x):
    return (x.replace("https://","").replace("http://","").replace("www.","").replace(".com/",".com").replace(".org/",".org").replace(".us/",".us").replace(".edu/",".edu")).lower()
df['Clean'] = df[list(df.columns)[1]].apply(clean)

# Create list with each protocol and www combo
def canon_status(x):
	headers = {'User-Agent': 'domain-health'}
	ver = ['http://' + x,'https://' + x,'http://www.' + x,'https://www.' + x]
	list = []
	for i in range(len(ver)):
		x = ver[i]
		try:
			z = requests.head(x, headers=headers).status_code
		except:
			pass
		list.append(x + ": " + str(z))
	return list
df['Versions and Status'] = df['Clean'].apply(canon_status)

# Show only the URLs that return a 200
def canon(x):
	headers = {'User-Agent': 'domain-health'}
	ver = ['http://' + x,'https://' + x,'http://www.' + x,'https://www.' + x]
	list = []
	for i in range(len(ver)):
		x = ver[i]
		try:
			z = requests.head(x, headers=headers).status_code
		except:
			pass
		if z == 200:
			list.append(x)
		else:
			pass
	if len(list) == 1:
		return list[0]
	elif len(list) == 0:
		return
	else:
		return list
df['Canonical'] = df['Clean'].apply(canon)

# Count the length of the canonical list
def count(x):
    try:
        return len(x)
    except:
        return 0
df['Count'] = df['Canonical'].apply(count)

# Set status based on length of list
def state(x):
    if x > 9:
        return "Good"
    elif x == 9:
        return "Timed out"
    elif x == 2 or x == 3 or x == 4:
        return "Multiple Resolving URLs"
    elif x == 0:
        return "No Canonical URL"
    else:
        pass
df['Canonical Status'] = df['Count'].apply(state)
###Canonical Domain Check

################################################################################

###Google Index
# Get quantity of indexed URLs
def index_count(x):
	try:
		list = []
		###Replace cx and key values when you setup your Google Custom Search Engine: https://serpstat.com/blog/retrieve-google-search-results-into-a-dataframe-using-python/
		df_index = adv.serp_goog("site:" + x, cx="", key="")
		tot = df_index['totalResults'].iloc[0]
		indexed = df_index['link'].iloc[0]
		pieces = indexed.split('/')
		rebuilt = pieces[0] + "//" + pieces[1] + pieces[2]
		list.append(int(tot))
		list.append(rebuilt)
		return list
	except:
		list = [0,x]
		return list
df['Google Index Info'] = df['Clean'].apply(index_count)

# Breaks up the List from index_count()
df['Google Index Count'] = df['Google Index Info'].str[0]
df['Google Indexed Domain'] = df['Google Index Info'].str[1]

# Checks to see if indexed domain version matches canonical domain
comparison_column = np.where(df["Google Indexed Domain"] == df["Canonical"], True, False)
df['Google Indexed Match'] = comparison_column

# Checks for 'noindex' in source code
def noindex(x):
	headers = {'User-Agent': 'domain-health'}
	try:
		r = requests.get(x, headers=headers)
		if "noindex " in r.text:
			return False
		else:
			return True
	except:
		return "No Canonical URL to Check"
df['Indexing Allowed?'] = df['Canonical'].apply(noindex)

df = df.drop(columns=['Clean','Count','Google Index Info'])
###Google Index

################################################################################

###Robots
# Creates the robots.txt URL
def robots(x):
	try:
		return x + "/robots.txt"
	except:
		try:
			return x[0] + "/robots.txt"
		except:
			return "No Robots.txt File Found"
df['Robots'] = df['Canonical'].apply(robots)

# Get robots.txt status code
df['Robots Status'] = df['Robots'].apply(url_access)

# Get robots.txt last mod date
df['Robots Last Mod'] = df['Robots'].apply(last_mod)

# Check robots.txt for disallow all directive
def robots_check(x):
  df_robots = robotstxt_to_df(x)
  df_robots['Concat'] = df_robots['directive'] + ": " + df_robots['content']
  df_robots = df_robots[df_robots['Concat'] == "Disallow: /"]
  try:
    da = df_robots['Concat'].iloc[0]
    return False
  except:
    return True
df['Crawling Allowed?'] = df['Robots'].apply(robots_check)
###Robots

################################################################################

###XML
# Creates XML sitemap URL
def xml(x):
	headers = {'User-Agent': 'domain-health'}
	try:
		try:
			r = requests.get(x + "/sitemap.xml", headers=headers)
			return r.url
		except:
			r = requests.get(x[0] + "/sitemap.xml", headers=headers)
			return r.url
	except:
		return "No XML Sitemap Found"
df['XML Sitemap'] = df['Canonical'].apply(xml)

# Get XML sitemap status code
df['XML Sitemap Status'] = df['XML Sitemap'].apply(url_access)

# Get XML sitemap URL count
def xml_count(x):
  try:
    df = sitemap_to_df(x)
    xml_urls = df['loc'].tolist()
    return len(xml_urls)
  except:
    return 0
df['XML Sitemap Count'] = df['XML Sitemap'].apply(xml_count)

# Get XML sitemap last mod date
df['XML Last Mod'] = df['XML Sitemap'].apply(last_mod)

# Checks to see if the index or XML sitemap URL count is higher
df['XML or Index'] = np.where((df['XML Sitemap Count'] - df['Google Index Count']) >= 0,"XML","Index")
df['% Diff'] = np.where((df['XML Sitemap Count'] - df['Google Index Count']) >= 0,(((df['XML Sitemap Count'] - df['Google Index Count']) / df['Google Index Count']) * 100).round(2).astype(str)+"%",(((df['Google Index Count'] - df['XML Sitemap Count']) / df['XML Sitemap Count']) * 100).round(2).astype(str)+"%")
###XML

################################################################################

# Sort by canonical domain status
df = df.sort_values(by=['Canonical Status'], ascending=False)

# Exports to Google Sheets
# Refer to this article for details on setting up Google Sheets Export: https://towardsdatascience.com/using-python-to-push-your-pandas-dataframe-to-google-sheets-de69422508f
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('jsonFileFromGoogle.json', scope)
gc = gspread.authorize(credentials)
###Replace the Google Sheets URL when you make your copy
spreadsheet_key = '1jAuoEFmslQRUijrPA3VLcVLEjCXXByQzlpueq-FTqHs'
wks_name = 'Report'
d2g.upload(df, spreadsheet_key, wks_name, credentials=credentials, row_names=True)

# Export Excel File as a backup and historical record
###Set file path for an Excel export
out_path = ""
writer = pd.ExcelWriter(out_path , engine='xlsxwriter')
df.to_excel(writer, sheet_name='Sheet1', index=False)
writer.save()

# Email Sender
###Set with your gmail address
sender_address = ''
###Set your gmail password
sender_pass = ''
recipients = ['you@example.com']
receiver_address = recipients
###Replace the Google Sheets and DataStudio URLs when you make your copy
mail_content = name + ',</br></br>The Domain Health Report is ready to review.</br></br>• <a href="https://datastudio.google.com/reporting/0c94cdfd-45a2-4850-9db9-e4f4fcc288cc">Dashboard</a></br></br>• <a href="https://docs.google.com/spreadsheets/d/1jAuoEFmslQRUijrPA3VLcVLEjCXXByQzlpueq-FTqHs/edit#gid=1581225624">Detailed Report</a></br></br>Check for the following:</br>• "Good" in Canonical Status</br>• Reasonable Amount of URLs in Google Index Count</br>• "True" in Goole Indexed Match</br>• "200" in Robots Status</br>• Crawlers Allow Set to "True"</br>• "200" in XML Status</br>• XML vs. Index Count Diff Review</br>• Indexing Allow Set to "True"</br></br>Reply-all to this email with any action items</br></br><a href="https://docs.google.com/spreadsheets/d/1jAuoEFmslQRUijrPA3VLcVLEjCXXByQzlpueq-FTqHs/edit#gid=0">Domain Health Input File</a>'
message = MIMEMultipart()
message['From'] = sender_address
message['To'] = ", ".join(recipients)
message['Subject'] = 'Domain Health Report'
message.attach(MIMEText(mail_content, 'html'))
session = smtplib.SMTP_SSL("smtp.gmail.com", 465)
session.login(sender_address, sender_pass)
text = message.as_string()
session.sendmail(sender_address, receiver_address, text)
session.quit()
print('Email Sent')
