from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta, date
from urllib.parse import urlparse
import logging
import requests
import pandas as pd
import re
import io
import os
import sys
import csv
import glob
import shutil

sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding='utf-8')

class TSDB:
	url = "https://www.thesportsdb.com/"
	login = f"{url}user_login.php"
	event = f"{url}event.php?e="
	edit_event = f"{url}edit_event.php?e="
	edit_event_add = f"{url}edit_event_player_result.php?e="
	edit_team_add = f"{url}edit_team_add.php?l="
	thumb = f"{url}uploadeventthumb.php?t="
	poster = f"{url}uploadeventposter.php?t="
	square = f"{url}uploadeventsquare.php?t="
	fanart = f"{url}uploadeventfanart.php?t="
	banner = f"{url}uploadeventbanner.php?t="
	edit_team = "edit_team.php?t="
	api = f"{url}api/v1/json/"
	with open("_config/TSDB_credentials.txt", "r") as f:
	  for details in f:
	    username, password, api_key = details.split(":")
	login = (f"{url}user_login.php")
	login_data = {"username": username, "password": password}

class whitelist:
	league_ids = [4546, 4547, 4548, 5277]

class dst1:
	anz = datetime.strptime("2023-10-01 02:00:00", '%Y-%m-%d %H:%M:%S')
	na = datetime.strptime("2023-11-05 02:00:00", '%Y-%m-%d %H:%M:%S')
	isr = datetime.strptime("2023-10-29 02:00:00", '%Y-%m-%d %H:%M:%S')
	eu = datetime.strptime("2023-10-29 01:00:00", '%Y-%m-%d %H:%M:%S')
	chl = datetime.strptime("2023-09-02 00:00:00", '%Y-%m-%d %H:%M:%S')

class dst2:
	anz = datetime.strptime("2024-04-07 03:00:00", '%Y-%m-%d %H:%M:%S')
	na = datetime.strptime("2024-03-10 02:00:00", '%Y-%m-%d %H:%M:%S')
	isr = datetime.strptime("2024-03-29 02:00:00", '%Y-%m-%d %H:%M:%S')
	eu = datetime.strptime("2024-03-31 01:00:00", '%Y-%m-%d %H:%M:%S')
	chl = datetime.strptime("2024-04-06 00:00:00", '%Y-%m-%d %H:%M:%S')

class files:
	urls_filesize = os.path.getsize('links.csv')
	tz_club = pd.read_csv('_config/clubs_tz.csv')
	tz_country = pd.read_csv('_config/countries_tz.csv')
	log_file = '_config/log.csv'
	cannot_find = '_config/cannot_find.csv'

def log_record(event_id, event_sport, event_name):
    # Write error message to a CSV file
    with open(files.log_file, 'a', newline='', encoding="utf8") as csvfile:
        fieldnames = ['Timestamp', 'Event ID', 'Sport', 'Event']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # If the file is empty, write headers
        if csvfile.tell() == 0:
            writer.writeheader()

        # Write the error details to the CSV file
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        writer.writerow({'Timestamp': timestamp,
        				'Event ID': event_id,
        				'Sport': event_sport,
        				'Event': event_name
        				})

def log_cannot_find(event_id, event_sport, hometeam, country):
	# Write error message to a CSV file
    with open(files.cannot_find, 'a', newline='', encoding="utf8") as csvfile:
        fieldnames = ['Timestamp', 'Event ID', 'Sport', 'Team Name', 'Country']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # If the file is empty, write headers
        if csvfile.tell() == 0:
            writer.writeheader()

        # Write the error details to the CSV file
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        writer.writerow({'Timestamp': timestamp,
        				'Event ID': event_id,
        				'Sport': event_sport,
        				'Team Name': hometeam,
        				'Country': country
        				})


def login():
	global driver
	service = Service()
	driver = webdriver.Chrome(service=service)
	driver.get(TSDB.login)
	driver.find_element(By.NAME, "username").send_keys(TSDB.username)
	driver.find_element(By.NAME, "password").send_keys(TSDB.password)
	driver.find_element(By.XPATH, "/html/body/section/div/div[3]/div/form/div[4]/input").click()
	return

def strip_url(full_url):
	if '/event/' in full_url:
		TSDB_event_ID = full_url.replace('https://www.thesportsdb.com/event/', '')
		return TSDB_event_ID
	if '/player/' in full_url:
		TSDB_player_ID = full_url.replace('https://www.thesportsdb.com/player/', '')
		TSDB_player_ID = TSDB_player_ID.split("-", 1)[0]
		return TSDB_player_ID
	if '/team/' in full_url:
		TSDB_team_ID = full_url.replace('https://www.thesportsdb.com/team/', '')
		TSDB_team_ID = TSDB_team_ID.split("-", 1)[0]
		return TSDB_team_ID
	else:
		print('Check the URL.')

def scrape_urls(no_days, sport):
	if files.urls_filesize > 1:
		print(f"Scrape already conducted, converting UTC to local datetimes now...")
		login()
		return
	else:
		login()

		for i in range(no_days):
			day_change = timedelta(days = i)
			date_today = datetime.now() + day_change
			date_today = date_today.strftime("%Y-%m-%d")

			driver.get(f"{TSDB.url}browse_calendar/?d={date_today}&s={sport}")

			urls_to_scrape = set()

			elems = driver.find_elements(By.XPATH, '//a[contains(@href,"event/")]')

			for elem in elems:
				urls_to_scrape.add(elem.get_attribute("href"))

			with open('links.csv', 'a', encoding="utf8", newline='') as f:
				writer = csv.writer(f)
				for url_to_scrape in urls_to_scrape:
					writer.writerow([url_to_scrape])

		return

def extract_ids_from_csv(csv_file):
    event_ids = []
    df = pd.read_csv(csv_file, header=None)

    for column in df.columns:
        event_ids.extend(re.findall(r'\d{7}', ' '.join(df[column])))

    return event_ids

def local_tz(no_days):
	scrape_urls(no_days, "Soccer")

	count = 0
	o_count = 0
	find_count = 0

	for event_id in extract_ids_from_csv('links.csv'):
		api_call = requests.get(f"{TSDB.api}{TSDB.api_key}/lookupevent.php?id={event_id}")
		if api_call.status_code == 200:
		    storage = api_call.json()
		    for event in storage["events"]:
		    	teamname = event["strHomeTeam"]
		    	team_id = event["idHomeTeam"]
		    	event_name = event["strEvent"]
		    	eventleagueid = event["idLeague"]
		    	event_sport = event["strSport"]
		    	country = event["strCountry"]
		    	postponed = event["strPostponed"]
		    	UTC_date = event["dateEvent"]
		    	UTC_time = event["strTime"]

		driver.get(f"{TSDB.edit_event}{event_id}")

		if postponed == "yes":
			count -= 1
			print(f"   POSTPONED - Event: {event_name} ({event_sport})")
			continue

		if UTC_time == "":
			count -= 1
			print(f"   NO UTC TIME - Event: {event_name} ({event_sport})")
			continue
		full_UTC_date_time = datetime.strptime(f"{UTC_date} {UTC_time}", '%Y-%m-%d %H:%M:%S')

		if country == "":
			api_call = requests.get(f"{TSDB.api}{TSDB.api_key}/lookupteam.php?id={team_id}")
			if api_call.status_code == 200:
			    storage_team = api_call.json()
			    for team in storage_team["teams"]:
			    	country = team["strCountry"]

			driver.find_element(By.NAME, "country").clear();
			driver.find_element(By.NAME, "country").send_keys(country)
			print(f"   CHANGED - Event: {event_name} ({event_sport})")
			if (full_UTC_date_time > dst2.eu):
				tz = files.tz_country.loc[files.tz_country["country"] == country, "summer"]
			if (full_UTC_date_time < dst2.eu):
				tz = files.tz_country.loc[files.tz_country["country"] == country, "winter"]

		if (teamname == "USA" or teamname == "Canada" or teamname == "Australia" or teamname == "Russia" or teamname == "Brazil" or teamname == "Mexico"):
			count -= 1
			continue

		### Event which have their country field set as Europe.
		### The home team's country is looked up via an API call this is added to the country field
		### and then this is used to work out the local time.
		if country == "Europe":
			if int(eventleagueid) in whitelist.league_ids:
				api_call = requests.get(f"{TSDB.api}{TSDB.api_key}/lookupteam.php?id={team_id}")
				if api_call.status_code == 200:
				    storage_team = api_call.json()
				    for team in storage_team["teams"]:
				    	country = team["strCountry"]

				driver.find_element(By.NAME, "country").clear();
				driver.find_element(By.NAME, "country").send_keys(country)
				print(f"   CHANGED - Event: {event_name} ({event_sport})")
				if (full_UTC_date_time > dst2.eu):
					tz = files.tz_country.loc[files.tz_country["country"] == country, "summer"]
				if (full_UTC_date_time < dst2.eu):
					tz = files.tz_country.loc[files.tz_country["country"] == country, "winter"]
			else:
				print(f"   EVENT ID {eventleagueid} needs to be added to whitelist.")
				continue


		elif country == "World":
			print(f"   {event_name} country is set to 'World'.")
			count -= 1
			continue
		
		### No DST countries with multiple timezones.
		elif (country == "Brazil" or country == "DR-Congo" or country == "Congo-DR" or country == "Indonesia" or country == "Kazakhstan" or country == "Russia"):
			tz = files.tz_club.loc[files.tz_club["team"] == teamname, "summer"]
			if tz.empty:
				print(f"   Could not find: {teamname} - {country}")
				log_cannot_find(event_id, event_sport, teamname, country)
				find_count += 1
				continue

		### DST countries with multiple timezones.
		### Australia
		elif (country == "Australia") and (full_UTC_date_time > dst1.anz):
		 	tz = files.tz_club.loc[files.tz_club["team"] == teamname, "summer"]
		 	if tz.empty:
		 		print(f"   Could not find: {teamname} - {country}")
		 		log_cannot_find(event_id, event_sport, teamname, country)
		 		find_count += 1
		 		continue

		elif (country == "Australia") and (full_UTC_date_time < dst1.anz):
		 	tz = files.tz_club.loc[files.tz_club["team"] == teamname, "winter"]
		 	if tz.empty:
		 		print(f"   Could not find: {teamname} - {country}")
		 		log_cannot_find(event_id, event_sport, teamname, country)
		 		find_count += 1
		 		continue

		### North America
		elif (country == "Canada" or country == "Mexico" or country == "USA" or country == "United States" or country == "United-States") and (full_UTC_date_time < dst1.na):
		 	tz = files.tz_club.loc[files.tz_club["team"] == teamname, "summer"]
		 	if tz.empty:
		 		print(f"   Could not find: {teamname} - {country}")
		 		log_cannot_find(event_id, event_sport, teamname, country)
		 		find_count += 1
		 		continue

		elif (country == "Canada" or country == "Mexico" or country == "USA" or country == "United States" or country == "United-States") and (full_UTC_date_time > dst1.na):
		 	tz = files.tz_club.loc[files.tz_club["team"] == teamname, "winter"]
		 	if tz.empty:
		 		print(f"   Could not find: {teamname} - {country}")
		 		log_cannot_find(event_id, event_sport, teamname, country)
		 		find_count += 1
		 		continue

		### Canary Islands
		elif (teamname == "CD Marino" or teamname == "Las Palmas" or teamname == "Las Palmas Atl" or teamname == "Tamaraceite" or teamname == "Tenerife" or teamname == "Santa Clara") and (full_UTC_date_time < dst1.eu):
			tz = files.tz_club.loc[files.tz_club["team"] == teamname, "summer"]
		elif (teamname == "CD Marino" or teamname == "Las Palmas" or teamname == "Las Palmas Atl" or teamname == "Tamaraceite" or teamname == "Tenerife" or teamname == "Santa Clara") and (full_UTC_date_time > dst1.eu):
			tz = files.tz_club.loc[files.tz_club["team"] == teamname, "winter"]
		

		### Countries with only one timezone.
		### Chile
		elif (country == "Chile") and (full_UTC_date_time < dst1.chl):
			tz = files.tz_country.loc[files.tz_country["country"] == country, "summer"]
		elif (country == "Chile") and (full_UTC_date_time > dst1.chl):
			tz = files.tz_country.loc[files.tz_country["country"] == country, "winter"]

		### New Zealand
		elif (country == "New Zealand" or country == "New-Zealand") and (full_UTC_date_time < dst1.anz):
			tz = files.tz_country.loc[files.tz_country["country"] == country, "summer"]
		elif (country == "New Zealand" or country == "New-Zealand") and (full_UTC_date_time > dst1.anz):
			tz = files.tz_country.loc[files.tz_country["country"] == country, "winter"]

		### North America
		elif (country == "Cuba" or country == "Antigua and Barbuda" or country == "Bahamas" or country == "Bermuda" or country == "Haiti" or country == "Turks and Caicos") and (full_UTC_date_time < dst1.na):
			tz = files.tz_country.loc[files.tz_country["country"] == country, "summer"]
		elif (country == "Cuba" or country == "Antigua and Barbuda" or country == "Bahamas" or country == "Bermuda" or country == "Haiti" or country == "Turks and Caicos") and (full_UTC_date_time > dst1.na):
			tz = files.tz_country.loc[files.tz_country["country"] == country, "winter"]

		### Israel
		elif (country == "Israel") and (full_UTC_date_time < dst1.isr):
			tz = files.tz_country.loc[files.tz_country["country"] == country, "summer"]
		elif (country == "Israel") and (full_UTC_date_time > dst1.isr):
			tz = files.tz_country.loc[files.tz_country["country"] == country, "winter"]

		### Europe
		elif (country == "Portugal" or country == "Spain" or country == "Palestine" or country == "Paraguay" or country == "Lebanon" or country == "Faroe Islands" or country == "Ireland" or country == "England" or country == "Scotland" or country == "Wales" or country == "Northern Ireland" or country == "Albania" or country == "Andorra" or country == "Austria" or country == "Belgium" or country == "Bosnia and Herzegovina" or country == "Croatia" or country == "Czech-Republic" or country == "Denmark" or country == "France" or country == "Germany" or country == "Gibraltar" or country == "Hungary" or country == "Italy" or country == "Kosovo" or country == "Liechtenstein" or country == "Luxembourg" or country == "Malta" or country == "Moldova" or country == "Montenegro" or country == "Netherlands" or country == "Macedonia" or country == "Morocco" or country == "Norway" or country == "Poland" or country == "San-Marino" or country == "Serbia" or country == "Slovakia" or country == "Slovenia" or country == "Sweden" or country == "Switzerland" or country == "Bulgaria" or country == "Cyprus" or country == "Estonia" or country == "Finland" or country == "Greece" or country == "Latvia" or country == "Lithuania" or country == "Romania" or country == "Ukraine") and (full_UTC_date_time < dst1.eu):
			tz = files.tz_country.loc[files.tz_country["country"] == country, "summer"]
		elif (country == "Portugal" or country == "Spain" or country == "Palestine" or country == "Paraguay" or country == "Lebanon" or country == "Faroe Islands" or country == "Ireland" or country == "England" or country == "Scotland" or country == "Wales" or country == "Northern Ireland" or country == "Albania" or country == "Andorra" or country == "Austria" or country == "Belgium" or country == "Bosnia and Herzegovina" or country == "Croatia" or country == "Czech-Republic" or country == "Denmark" or country == "France" or country == "Germany" or country == "Gibraltar" or country == "Hungary" or country == "Italy" or country == "Kosovo" or country == "Liechtenstein" or country == "Luxembourg" or country == "Malta" or country == "Moldova" or country == "Montenegro" or country == "Netherlands" or country == "Macedonia" or country == "Morocco" or country == "Norway" or country == "Poland" or country == "San-Marino" or country == "Serbia" or country == "Slovakia" or country == "Slovenia" or country == "Sweden" or country == "Switzerland" or country == "Bulgaria" or country == "Cyprus" or country == "Estonia" or country == "Finland" or country == "Greece" or country == "Latvia" or country == "Lithuania" or country == "Romania" or country == "Ukraine") and (full_UTC_date_time > dst1.eu):
			tz = files.tz_country.loc[files.tz_country["country"] == country, "winter"]

		### All Other Countries
		elif (full_UTC_date_time < dst1.eu):
			tz = files.tz_country.loc[files.tz_country["country"] == country, "summer"]
		elif (full_UTC_date_time > dst1.eu):
			tz = files.tz_country.loc[files.tz_country["country"] == country, "winter"]

		else:
			count -= 1
			continue

		timedelta_values = tz.apply(lambda x: timedelta(hours=x))

		local_date_time = full_UTC_date_time + timedelta_values

		local_date = local_date_time.apply(lambda x: x.strftime("%Y-%m-%d"))
		local_time_h = local_date_time.apply(lambda x: x.strftime("%H"))
		local_time_m = local_date_time.apply(lambda x: x.strftime("%M"))

		driver.find_element(By.NAME, "dateeventlocal").clear();
		driver.find_element(By.NAME, "timeeventlocal").clear();

		driver.find_element(By.NAME, "dateeventlocal").send_keys(local_date)
		driver.find_element(By.NAME, "timeeventlocal").send_keys(local_time_h)
		driver.find_element(By.NAME, "timeeventlocal").send_keys(local_time_m)
		driver.find_element(By.NAME, "submit").click()

		tz_str = str(tz.iloc[0]) 
		print(f"   Local datetime updated for {event_name} ({event_sport}).")
		log_record(event_id, event_sport, event_name)
		count += 1
		o_count+= 1

	if count == 1:
	    print(f"\n{count} of {o_count} datetime updated.")
	else:
	    print(f"\n{count} of {o_count} datetimes updated.")

	if find_count == 0:
		print(f"\nNo teams missing.")
	if find_count == 1:
	    print(f"\n{find_count} team could not be found.")
	else:
	    print(f"\n{find_count} teams could not be found.")

	driver.quit()

local_tz(5)