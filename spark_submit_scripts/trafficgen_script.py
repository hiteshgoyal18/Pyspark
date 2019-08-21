# -*- coding: utf-8 -*-
#!/usr/bin/python
import sys, time, os, re, string, logging, datetime, hashlib, socket, json, traceback
from random import randint
from uuid import getnode as get_mac
import multiprocessing
import requests
import collections
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
from db_config import *

# --------------------------------------------------#
#-- load config and setup variables ---#
#-- sample call (python save_to_db.py 1 normal_ip)
job_id = sys.argv[1] #-- get conf to select
ip_type = sys.argv[2] #-- get ip type used (normal_ip/microleaves)

payload = {'call':'select_config','job_id': job_id}
r = requests.get(db_operation_url, params=payload)
r.encoding = 'utf-8'
result = r.text
temp = json.loads(result)
config = temp[0]

print "configs loaded successfully..."
ua=''
proxy_ip=''
total_no_of_ads_to_click = 0
ad_format = []
if config['click_ads'] == '1':
	total_no_of_ads_to_click = config['no_of_ads_to_click']

payload = {'call':'surf_website_result','id': config['campaign_id']}
r = requests.get(db_operation_url, params=payload)
r.encoding = 'utf-8'
result = r.text
temp = json.loads(result)
surf_website = temp #-- get all surf websites..

#--------X-------------X-----------------------------------------------XX-----------------------------------------------XX-------------------------X--------------------X---#

def get_browser_and_start(que,total_no_of_ads_to_click):
	while not que==0:
		#-- logging --#
		local_sess_id_hash = hashlib.md5()
		local_sess_id_hash.update("trafficgen_"+time.strftime("%Y%m%d%H%M%S")+str(randomNo(0,100))+str(randomNo(101,200))+str(randomNo(0,999)))
		print local_sess_id_hash.hexdigest()
		local_sess_id = local_sess_id_hash.hexdigest()
		#-- logging --#
		local_conf = {}
		local_conf['local_sess_id'] = local_sess_id	
		
		proxy_ip=''
		if config['use_proxy'] == '1':
			ip_list = ['223.196.75.63:8080','14.140.110.198:40672','103.24.108.238:8080','61.16.141.85:3128','111.90.175.34:45454','139.59.77.126:3128','103.240.100.106:8080','45.64.213.130:53281','45.123.3.105:8080','14.141.93.162:8080','27.54.170.6:45454','124.124.1.178:3128','103.38.89.101:3128','182.74.209.190:3128','119.235.55.152:8080','218.248.73.193:1080','175.111.129.195:53281','103.78.22.34:53281','125.62.193.18:53281','203.122.23.41:27601','202.56.203.40:80','115.115.75.158:8080','103.57.134.53:8080','125.20.209.22:8080','1.186.45.170:8080']
			ip_rnd = randomNo(0,len(ip_list)-1)
			proxy_ip = ip_list[ip_rnd]

		if config['spoof_user_agent'] == '1':
			user_agent=''
		else:
			user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.1 Safari/603.1.30'

		#-- for temp usage --##
		uas=[]
		with open(ua_file,'r') as file:
			for line in file:
				if(line.rstrip('\n') not in uas):
					uas.append(line.rstrip('\n'))
		ualen = len(uas)-1
		user_agent = uas[randomNo(0,ualen)]
		#-- for temp usage --##

		print user_agent
		local_conf['user_agent'] = user_agent
		if config['delay_per_visit'] > '0':
			time.sleep(int(config['delay_per_visit'])) #-- put delay in each visit
		
		print "before ghost try ..."
		try:
			if config['use_ghost'] == '1':
				if config['use_proxy'] == '1' and config['spoof_user_agent'] == '1':
					#-- set up proxy and user agent variables for phantomjs starts ---#
					dcap = dict(DesiredCapabilities.PHANTOMJS)
					dcap["phantomjs.page.settings.javascriptEnabled"] = True
					dcap["phantomjs.page.settings.userAgent"] = (user_agent)
					service_args = [
					'--proxy='+proxy_ip,
					'--proxy-type=http',
					]
					#-- set up proxy and user agent variables for phantomjs ends ---#
					browser = webdriver.PhantomJS(desired_capabilities=dcap,service_args=service_args)
				else:
					browser = webdriver.PhantomJS()
			else:
				if config['use_proxy'] == '1' and config['spoof_user_agent'] == '1':
					#-- set up proxy and user agent variables for chrome starts ---#
					options = webdriver.ChromeOptions()
					options.binary_location = '/opt/google/chrome/google-chrome'
					options.add_argument('--profile-directory=Default')
					options.add_argument('--user-agent='+user_agent)
					options.add_argument('--proxy-server=http://%s' % proxy_ip)
					options.add_argument("--no-sandbox")
					
					# -- uncomment to disable flash ro plugins--
					options.add_argument('--disable-extensions')
					options.add_argument("--disable-automation")
					options.add_argument("--disable-infobars")
					options.add_argument("--disable-internal-flash")
					options.add_argument("--disable-plugins-discovery")
					browser = webdriver.Chrome(chrome_options=options)
				else:
					options = webdriver.ChromeOptions()
					options.binary_location = '/opt/google/chrome/google-chrome'
					options.add_argument("--no-sandbox")
					browser = webdriver.Chrome(chrome_options=options)
		except:
			print 'exit'
			sys.exit()

		if browser:
			bm = randomNo(0,1)
			if bm == 1:
				browser.maximize_window()
			#-- delete cookie (0-> never, 1-> always, 2-> random)
			if config['remove_cookie'] == '1':
				browser.delete_all_cookies() #-- clear all cookies
				print "cookie removed..."
			if config['remove_cookie'] == '2':
				cdel = randomNo(0,1)
				if cdel == 1:
					browser.delete_all_cookies() #-- clear all cookies
					print "cookie removed..."

			#-- insert start --#
			try:
				browser.get('http://69.30.210.90/ip_chk.php')
				div = WebDriverWait(browser, 10,3).until(lambda x: x.find_element_by_tag_name("div"))
				if div:
					c_ip = browser.find_element_by_tag_name("div")
					local_conf['ip'] = c_ip.text
			except:
				browser.quit()
			#-- insert end --#

			time.sleep(5)
			surfwebsite(browser,config,local_conf)
			
			# -- the main part..
			time.sleep(5)
			local_conf['total_no_of_ads_to_click'] = int(total_no_of_ads_to_click)
			local_conf['ad_clicks_remaining'] = int(total_no_of_ads_to_click)
			local_conf['surf'] = 0
			openurl(browser,config,local_conf) #-- call function
			
			time.sleep(5)
			surfwebsite(browser,config,local_conf)

			time.sleep(5)
			browser.quit()
		else:
			sys.exit()
		que=que-1

def surfwebsite(browser,config,local_conf):
	print "in surfwebsite"
	count = len(surf_website)
	if count == 1 and surf_website[0]['website_url'] == '':
		return
	no_of_visits = count/100
	if no_of_visits < 1:
		no_of_visits = 1

	for i in range(no_of_visits):
		surf_no = randomNo(0,count-1)
		local_conf['url'] = surf_website[surf_no]['website_url']
		local_conf['surf'] = 1
		local_conf['list_to_skip'] = ''
		openurl(browser,config,local_conf) #-- call function
	return

def openurl(browser,config,local_conf):
	try:
		action = ActionChains(browser)
		# -- part for only surfing websites..
		if local_conf['surf'] == 1:
			browser.get(local_conf['url']) #-- open url
			body = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_tag_name("body"))
			if body:
				print browser.current_url

				# -- db insert --
				payload = {'call':'insert_db','job_id': config['job_id'],'session_id':local_conf['local_sess_id'],'url':browser.current_url,'ip':local_conf['ip'],'ua':local_conf['user_agent'],'surf_url':local_conf['surf']}
				r = requests.get(db_operation_url, params=payload)
				r.encoding = 'utf-8'
				_insert = r.text
				# -- db insert --

				if config['random_mouse_move'] == '1':
					movemouse(browser,'') #-- perform random scroll
				if config['click_inner_links'] == '1':
					click_rep = randomNo(1,3) #-- how many inner links to click in a session
					for click in range(click_rep):
						clickinnerlinks(browser,config,local_conf)
						# -- db insert --
						payload = {'call':'insert_db','job_id': config['job_id'],'session_id':local_conf['local_sess_id'],'url':browser.current_url,'ip':local_conf['ip'],'ua':local_conf['user_agent'],'surf_url':local_conf['surf']}
						r = requests.get(db_operation_url, params=payload)
						r.encoding = 'utf-8'
						_insert = r.text
						# -- db insert --
			return
		# -- part for visiting targeted website ..
		else:
			if config['referer'] == '1':
				payload = {'call':'referer_result','id': config['campaign_id']}
				r = requests.get(db_operation_url, params=payload)
				r.encoding = 'utf-8'
				result = r.text
				temp = json.loads(result)
				all_referer = temp #-- get all referers..
				ref_len = len(all_referer)
				ref_id = randint(1,ref_len)
				# ref_id=3 #-- for temp now
				
				payload = {'call':'keyword_result','id': ref_id}
				r = requests.get(db_operation_url, params=payload)
				r.encoding = 'utf-8'
				result = r.text
				temp = json.loads(result)
				all_keywords = temp #-- get all keywords associated to selected referer..
				if len(all_keywords) > 1:
					rnd_key = randomNo(0,len(all_keywords)-1)
				else:
					rnd_key = 0
				search_website = all_keywords[rnd_key]['referer_website']
				search_key_word = all_keywords[rnd_key]['keyword']
				print "website -> "+search_website
				print search_key_word
				# ------------------------------- #
				if search_website == 'https://google.co.in':
					browser.get(search_website)
					body = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_tag_name("body"))
					if body:
						search = browser.find_element_by_name('q')
						search.send_keys(search_key_word)
						search.send_keys(Keys.RETURN) # hit return after you enter search text
						body = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_css_selector('div.g'))
						if body:
							div_to_click = browser.find_elements_by_css_selector('div.g')
							print len(div_to_click)
							url_to_click = div_to_click[0].find_element_by_tag_name("a")
							print url_to_click.get_attribute("href")
							action.click(url_to_click) #-- click on first link
							action.perform()

				if search_website == 'https://www.facebook.com':
					browser.get(search_website+"/directory/pages/")
					body = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_tag_name("body"))
					if body:
						search = browser.find_element_by_name('q')
						search.send_keys(search_key_word)
						search.send_keys(Keys.RETURN) # hit return after you enter search text

						# -- search for captcha..
						try:
							captcha = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_css_selector('div.captcha'))
							if captcha:
								return
						except:
							body = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_css_selector('div.instant_search_title'))
							if body:
								div_to_click = browser.find_elements_by_css_selector('div.instant_search_title')
								print len(div_to_click)
								url_to_click = div_to_click[0].find_element_by_tag_name("a")
								print url_to_click.get_attribute("href")
								action.click(url_to_click) #-- click on first link
								action.perform()

								# -- search for captcha..
								try:
									captcha = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_css_selector('div.captcha'))
									if captcha:
										return
								except:
									body = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_tag_name("body"))
									if body:
										try:
											browser.execute_script("document.getElementById('u_0_41').style.display = 'none';")
										except:
											return

										# -- search for links to the targeted website..
										string_to_match = string.replace(string.replace(string.replace(string.replace(config['site_url'], 'http://', ''),'www.',''),'https://',''),'/','')
										print string_to_match
										try:
											website_link = browser.find_elements_by_xpath("//a[contains(@href, '"+string_to_match+"')]")
											ref_rand = randomNo(0,len(website_link)-1)
											print website_link[ref_rand].get_attribute('href')
											action.click(website_link[ref_rand]) #-- click on random link
											action.perform()
										except:
											website_link = browser.find_elements_by_xpath("//a[contains(@href, '"+string_to_match+"')]")
											ref_rand = randomNo(0,len(website_link)-1)
											browser.get(website_link[ref_rand].get_attribute('href'))
											

				if search_website == 'https://in.yahoo.com/':
					browser.get(search_website)
					body = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_tag_name("body"))
					if body:
						search = browser.find_element_by_name('p')
						search.send_keys(search_key_word)
						search.send_keys(Keys.RETURN) # hit return after you enter search text
						body = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_css_selector('div.fst'))
						if body:
							div_to_click = browser.find_elements_by_css_selector('div.fst')
							print len(div_to_click)
							url_to_click = div_to_click[1].find_element_by_tag_name("a")
							print url_to_click.get_attribute("href")
							action.click(url_to_click) #-- click on first link
							action.perform()

				if search_website == 'Direct':
					browser.get(config['site_url']) #-- open url
			else:
				browser.get(config['site_url']) #-- open url

			#-- check if url is loaded properly --#
			body = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_tag_name("body"))
			if body:
				try:
					handles = len(browser.window_handles)
					if handles > 1:
						browser.switch_to_window(browser.window_handles[1])
				except:
					print "error finding window handles"

				print browser.current_url

				# -- db insert --
				payload = {'call':'insert_db','job_id': config['job_id'],'session_id':local_conf['local_sess_id'],'url':browser.current_url,'ip':local_conf['ip'],'ua':local_conf['user_agent'],'surf_url':local_conf['surf']}
				r = requests.get(db_operation_url, params=payload)
				r.encoding = 'utf-8'
				_insert = r.text
				# -- db insert --

				if config['random_mouse_move'] == '1':
					movemouse(browser,'') #-- perform random scroll

				if config['click_ads'] == '1':
					# -- get ad params from db start..
					payload = {'call':'ad_result','id': config['campaign_id']}
					r = requests.get(db_operation_url, params=payload)
					r.encoding = 'utf-8'
					result = r.text
					temp = json.loads(result)
					ad_params_array = temp
					local_conf['ad_params_array'] = ad_params_array
					consumed_ad = []
					# -- get ad params from db end..

					if len(ad_params_array) > 0:
						local_conf['visible_ad_array'] = verifyvisiblead(browser,config,local_conf)
						if len(local_conf['visible_ad_array']) > 0:
							if len(local_conf['visible_ad_array']['ad_count']) > 0:
								ad_array = []
								for key, value in local_conf['visible_ad_array']['ad_count'].iteritems():
									ad_array.append(key) #-- add ad_provider for selecting elements.. 
									# -- db insert --
									ad_p = key
									ad_c = value
									payload = {'call':'insert_ad_temp','job_id': config['job_id'],'session_id':local_conf['local_sess_id'],'url':browser.current_url,'ip':local_conf['ip'],'ad_provider':ad_p,'total_ad_found':ad_c}
									r = requests.get(db_operation_url, params=payload)
									r.encoding = 'utf-8'
									_insert = r.text
									# -- db insert --
								if len(local_conf['visible_ad_array']['element_array']) > 0:
									for key,value in local_conf['visible_ad_array']['element_array'].iteritems():
										local_conf['ad_to_click'] = local_conf['visible_ad_array']['element_array'][key]
										local_conf['selected_ad_platform'] = key					
										local_conf['ad_clicks_remaining'] = clickonad(browser,config,local_conf)
										if local_conf['ad_clicks_remaining'] != '-1':
											consumed_ad.append(key) #-- to keep record of consumed ad click..
											break

				if config['click_inner_links'] == '1':
					local_conf['list_to_skip'] = config['urls_to_skip'].split(",")
					print len(local_conf['list_to_skip'])
					click_rep = randomNo(int(config['minimum_inner_link_clicks']),int(config['maximum_inner_link_clicks'])) #-- how many inner links to click in a session
					print "clickrep ==> ",click_rep
					for click in range(click_rep):
						string_to_match = string.replace(string.replace(string.replace(string.replace(config['site_url'], 'http://', ''),'www.',''),'https://',''),'/','')
						if string_to_match in browser.current_url:
							local_conf['url'] = config['site_url']
							clickinnerlinks(browser,config,local_conf)
							movemouse(browser,'')
							time.sleep(5)
							body = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_tag_name("body"))
							if body:
								# -- db insert --
								payload = {'call':'insert_db','job_id': config['job_id'],'session_id':local_conf['local_sess_id'],'url':browser.current_url,'ip':local_conf['ip'],'ua':local_conf['user_agent'],'surf_url':local_conf['surf']}
								r = requests.get(db_operation_url, params=payload)
								r.encoding = 'utf-8'
								_insert = r.text
								# -- db insert --

								if config['click_ads'] == '1':
									if len(ad_params_array) > 0:
										# -- get ad params from db start..
										payload = {'call':'ad_result','id': config['campaign_id']}
										r = requests.get(db_operation_url, params=payload)
										r.encoding = 'utf-8'
										result = r.text
										temp = json.loads(result)
										ad_params_array = temp
										local_conf['ad_params_array'] = ad_params_array
										# -- get ad params from db end..

										if len(ad_params_array) > 0:
											local_conf['visible_ad_array'] = verifyvisiblead(browser,config,local_conf)
											if len(local_conf['visible_ad_array']) > 0:
												if len(local_conf['visible_ad_array']['ad_count']) > 0:
													ad_array = []
													for key, value in local_conf['visible_ad_array']['ad_count'].iteritems():
														ad_array.append(key) #-- add ad_provider for selecting elements.. 
														# -- db insert --
														ad_p = key
														ad_c = value
														payload = {'call':'insert_ad_temp','job_id': config['job_id'],'session_id':local_conf['local_sess_id'],'url':browser.current_url,'ip':local_conf['ip'],'ad_provider':ad_p,'total_ad_found':ad_c}
														r = requests.get(db_operation_url, params=payload)
														r.encoding = 'utf-8'
														_insert = r.text
														# -- db insert --
													if len(local_conf['visible_ad_array']['element_array']) > 0:
														for key, value in local_conf['visible_ad_array']['element_array'].iteritems():
															if key not in consumed_ad:
																if local_conf['ad_clicks_remaining'] != '-1':
																	local_conf['ad_clicks_remaining'] = local_conf['total_no_of_ads_to_click']-local_conf['ad_clicks_remaining']
																local_conf['ad_to_click'] = local_conf['visible_ad_array']['element_array'][key]
																local_conf['selected_ad_platform'] = key
																local_conf['ad_clicks_remaining'] = clickonad(browser,config,local_conf)
																if local_conf['ad_clicks_remaining'] != '-1':
																	consumed_ad.append(key) #-- to keep record of consumed ad click..
																	break
		time.sleep(10)
		return
	except:
		return

def clickinnerlinks(browser,config,local_conf):
	print "get innerlink list by xpath"
	string_to_match = string.replace(string.replace(string.replace(local_conf['url'], 'http://', ''),'www.',''),'https://','')
	print string_to_match
	try:
		ele = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_xpath("//a[contains(@href, '"+string_to_match+"')]"))
		if ele:
			local_conf['innerlink'] = browser.find_elements_by_xpath("//a[contains(@href, '"+string_to_match+"')]")
			time.sleep(randomNo(3,5))
			performclick(browser,config,local_conf,0)
		return
	except:
		return

def performclick(browser,config,local_conf,retry=0):
	if retry < 5: #-- try maximum 5 times on exception
		action = ActionChains(browser)
		innerlink_len = len(local_conf['innerlink']) #-- get innerlink array size
		print innerlink_len
		rnd = randomNo(0,innerlink_len-1)
		print "try to click on inner link"
		try:
			time.sleep(randomNo(2,5))
			skip = 0
			link = 0
			click_elem = local_conf['innerlink'][rnd].get_attribute('href')
			print click_elem

			if local_conf['list_to_skip'] != '':
				for link in range(len(local_conf['list_to_skip'])-1):
					if local_conf['list_to_skip'][link] in str(click_elem):
						skip = 1
			if str(browser.current_url) == str(click_elem):
				skip = 1
			print "skip is => "+str(skip)

			if skip == 0:
				print click_elem
				movemouse(browser,local_conf['innerlink'][rnd])
				action.click(local_conf['innerlink'][rnd]) #-- click on random link
				action.perform()
				print "link clicked.."
				return
			else:
				performclick(browser,config,local_conf,retry)
		except:
			retry += 1
			performclick(browser,config,local_conf,retry)
	else:
		return

def verifyvisiblead(browser,config,local_conf):
	time.sleep(10)
	a_len = len(local_conf['ad_params_array'])
	count = collections.defaultdict(lambda: collections.defaultdict(dict))
	if a_len > 0:
		for i in range(a_len):
			count['ad_count'][local_conf['ad_params_array'][i]['ad_platform']] = 0
			try:
				selected_search_keyword = local_conf['ad_params_array'][i]['ad_keyword']
				try:
					try:
						chk_ad_elem = WebDriverWait(browser,5,3).until(lambda x: x.find_elements_by_css_selector(selected_search_keyword))
					except:
						chk_ad_elem = WebDriverWait(browser,5,3).until(lambda x: x.find_elements_by_xpath(selected_search_keyword))
				except:
					pass
				if chk_ad_elem:
					ad_elem = browser.find_elements_by_css_selector(selected_search_keyword)
					ad_elem = chk_ad_elem
					alen = len(ad_elem)
					for e in range(alen):
						adsize = ad_elem[e].size
						if int(adsize['width']) > 11 and int(adsize['height']) > 11:
							count['ad_count'][local_conf['ad_params_array'][i]['ad_platform']] += 1 #-- count ad display provider vise
							count['element_array'][local_conf['ad_params_array'][i]['ad_platform']][e] = ad_elem[e] #-- collect visible ads
						else:
							continue
				else:
					continue
			except:
				continue
	else:
		return count
	return count


def clickonad(browser,config,local_conf):
	action = ActionChains(browser)
	print "now search for ads---"
	print "before ad click remaining ==> "+str(local_conf['ad_clicks_remaining'])
	print "total ad clicks remaining ==> "+str(local_conf['total_no_of_ads_to_click'])
	if local_conf['ad_clicks_remaining'] <= local_conf['total_no_of_ads_to_click'] and local_conf['ad_clicks_remaining'] > -1:
		print "after ad click remaining ==> "+str(local_conf['ad_clicks_remaining'])
		selected_ad_platform = local_conf['selected_ad_platform']
		chk_ad_elem = local_conf['ad_to_click']
		try:
			if chk_ad_elem:
				ad_elem = chk_ad_elem
				print selected_ad_platform+" ads present ==> "+str(len(ad_elem))

				alen = len(ad_elem)-1
				if alen != 0:
					arnd = randomNo(0,alen)
				else:
					arnd = 0
				time.sleep(randomNo(2,5))
				print "click on ad no ==> "+str(arnd+1)
				adloc = ad_elem[arnd].location
				print adloc
				adsize = ad_elem[arnd].size
				print adsize

				if int(adsize['width']) > 11:
					offset_x = randint(10,int(adsize['width'])-10)
				else:
					offset_x = 15
				if int(adsize['height']) > 11:
					offset_y = randint(10,int(adsize['height'])-10)
				else:
					offset_y = 15
				print "offset x ==> "+str(offset_x)+", offset y ==> "+str(offset_y)
				

				if int(adsize['width']) > 15 and int(adsize['height']) > 15:
					action.move_to_element_with_offset(ad_elem[arnd], offset_x, offset_y).click().perform()
					time.sleep(2)
					try:
						handles = len(browser.window_handles)
						print "window handles found ==> "+str(handles)
						if handles > 1:
							print "add clicked..."

							# -- db insert --
							payload = {'call':'insert_db','job_id': config['job_id'],'session_id':local_conf['local_sess_id'],'url':browser.current_url,'ip':local_conf['ip'],'ua':local_conf['user_agent'],'surf_url':'0','ad_provider':selected_ad_platform,'ad_clicked':'1','ad_not_clicked':'0','total_ad_found':len(ad_elem)}
							r = requests.get(db_operation_url, params=payload)
							r.encoding = 'utf-8'
							_insert = r.text
							# -- db insert --

							local_conf['ad_clicks_remaining'] = local_conf['total_no_of_ads_to_click']-1
							if config['follow_landing_page'] == '1':
								follow_landing_page(browser,config,end_process=0)
						else:
							string_to_match = string.replace(string.replace(string.replace(string.replace(config['site_url'], 'http://', ''),'www.',''),'https://',''),'/','')
							if string_to_match not in browser.current_url:
								print "add clicked..."

								# -- db insert --
								payload = {'call':'insert_db','job_id': config['job_id'],'session_id':local_conf['local_sess_id'],'url':browser.current_url,'ip':local_conf['ip'],'ua':local_conf['user_agent'],'surf_url':'0','ad_provider':selected_ad_platform,'ad_clicked':'1','ad_not_clicked':'0','total_ad_found':len(ad_elem)}
								r = requests.get(db_operation_url, params=payload)
								r.encoding = 'utf-8'
								_insert = r.text
								# -- db insert --

								local_conf['ad_clicks_remaining'] = local_conf['total_no_of_ads_to_click']-1
								if config['follow_landing_page'] == '1':
									follow_landing_page(browser,config,end_process=1)
					except:
						print "error finding window handles"
						pass
					return local_conf['ad_clicks_remaining']
				else:
					print "ad is not displayed.."
					# -- db insert --
					payload = {'call':'insert_db','job_id': config['job_id'],'session_id':local_conf['local_sess_id'],'url':browser.current_url,'ip':local_conf['ip'],'ua':local_conf['user_agent'],'surf_url':'0','ad_provider':selected_ad_platform,'ad_clicked':'0','ad_not_clicked':'1','total_ad_found':len(ad_elem)}
					r = requests.get(db_operation_url, params=payload)
					r.encoding = 'utf-8'
					_insert = r.text
					# -- db insert --
					return '-1'
		except:
			print "exception occured in ad click"
			# -- db insert --
			payload = {'call':'insert_db','job_id': config['job_id'],'session_id':local_conf['local_sess_id'],'url':browser.current_url,'ip':local_conf['ip'],'ua':local_conf['user_agent'],'surf_url':'0','ad_provider':selected_ad_platform,'ad_clicked':'0','ad_not_clicked':'1','total_ad_found':'0'}
			r = requests.get(db_operation_url, params=payload)
			r.encoding = 'utf-8'
			_insert = r.text
			# -- db insert --
			pass
	else:
		return '-1'

def follow_landing_page(browser,config,end_process):
	try:
		body = WebDriverWait(browser, 10,3).until(lambda x: x.find_elements_by_tag_name("body"))
		if body:
			if end_process == 1:
				movemouse(browser,'')
				return
			else:
				browser.switch_to_window(browser.window_handles[1])
				time.sleep(2)
				print browser.title
				movemouse(browser,'')
				browser.close()
		return
	except:
		if end_process == 1:
			return
		else:
			browser.close()
		return

def randomNo(start,end):
	if(end == 0):
		return randint(0,9)
	else:
		return randint(start,end)

def doscroll(browser,elem):
	if(elem == ''):
		scrollpart = randomNo(0,4)
		browser.execute_script("window.scrollTo(0, "+str(scrollpart*500)+")")
	else:
		browser.execute_script("return arguments[0].scrollIntoView();", elem)
	return

def movemouse(browser,element):
	action = ActionChains(browser)
	if(element == ''):
		try:
			a_tags = browser.find_elements_by_xpath("//a[@href]")
			total_a = len(a_tags)
			print "total href found =>"
			print total_a
			mov = randomNo(5,8)
			count = 0
			my = 0
			my1 = 0
			while(count <= mov):
				el = randint(0,total_a-1)
				my = a_tags[el].location.items()[0][1]
				if(my1 != 0):
					if(my < my1):
						action.move_to_element_with_offset(a_tags[el], 5, 5)
						action.perform()
						time.sleep(randomNo(2,5))
						count += 1
						my1 = my
					else:
						my1 = my
						continue
				else:
					my1 = a_tags[el].location.items()[0][1]
		except:
			pass
	else:
		action.move_to_element_with_offset(element,5, 5)
		action.perform()
	return

# --------------------##----------------------##------------------------------##-------------------------------------##---------------------------#
# --------------------##----------------------##------------------------------##-------------------------------------##---------------------------#

if __name__ == "__main__":
	thread_list = []
	total_visits=int(config['total_traffic'])-int(config['consumed_traffic']) #-- no of total_visits
	if total_visits > 0:
		max_threads=int(config['max_threads']) #-- no of max_threads
		visit_per_thread = total_visits/max_threads
		print visit_per_thread
		for i in range(max_threads):
			t = multiprocessing.Process(target=get_browser_and_start, args=[visit_per_thread,total_no_of_ads_to_click,])
			thread_list.append(t)
			print "starting thread..."
			t.daemon = True
			t.start()

		for t in thread_list:
			print "joning existing thread..."
			t.join()
	else:
		sys.exit()
