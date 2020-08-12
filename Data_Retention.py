#!/usr/bin/python3

import os
import json
import subprocess
import time
import datetime 
import threading
import shutil
import mysql.connector
from mysql.connector import Error
import logging
from queue import Queue



########################################################Checking Database############################################################################################
#This part of the scripts do two works first access the mattermost database and second assign a particular date for data retention and for accessing database you have to provide the database "user" and "passowrd". You have to give the "day" after that you want to delete mattermost data.
class Data_Retention:


	def __init__(self, flag):
		self.flag = flag
		 
	def db(self):
		try:	
			dbuser = "root"
			dbpass = "redhat"
			dbip = "127.0.0.1"
			
			RET_DATE=subprocess.check_output(['date  --date="0 day ago"  "+%s%3N"'], shell=True)
			RET_DATE=RET_DATE.decode("utf-8")
			RET_DATE=RET_DATE.replace(',', '')
			RET_DATE=RET_DATE.replace('"', '')
			RET_DATE=RET_DATE.replace(" ", "")
			self.RET_DATE=RET_DATE.strip("\n")
			#print(self.RET_DATE)
			self.connection = mysql.connector.connect(host=dbip,
                                         	user=dbuser,
					 	database='mattermost',
	                                 	password=dbpass)
			if self.connection.is_connected():
				#print('Connection to DB is [OK]')	
				self.cursor = self.connection.cursor()
				
				
		except Error as e:
			print(e)
#####################################################################################################################################################################
# In this part of script query the date of the data so that which we want to delete after 15 days.

	def Date_Query(self):
		CreateAt_Lists = []
		self.Final_Date_Lists = []			
		query_1 = """ select CreateAt from Posts """
		query_2 = """ select CreateAt from FileInfo """
		for tables in query_1,query_2:
			self.cursor.execute(tables)
			for row in self.cursor.fetchall():
				row =''.join(str(row))
				row = row.replace(',', '')
				row = row.replace('(', '')
				row = row.replace(')', '')
				CreateAt_Lists.append(row)
		NUM = len(CreateAt_Lists)
		for num in range(NUM):
			if CreateAt_Lists[num][0:5] == self.RET_DATE[0:5] and CreateAt_Lists[num][0:4] == self.RET_DATE[0:4]:
				#	print(epdata[num])
				self.Final_Date_Lists.append(CreateAt_Lists[num])
				#print(self.Final_Date_Lists)
		print("Finding Datestamp is [OK]")

#####################################################################################################################################################################
#In this part of script works on preserving channels data which is present in mattermost and you have to assign channel name in "self.channel" variable.

	def Channel_Id(self):
		self.channels = []
		self.channel = ''
		Channels = """select Id from Channels where DisplayName = %s"""
		self.cursor.execute(Channels, (self.channel,))
		file_info = self.cursor.fetchall()
		for y in file_info:
			y=''.join(y)
			self.channels.append(y)
		#print(self.channels)
		print("Finding ChanneId is [OK]")
######################################################################################################################################################################
#In this part of script if we want to preserve any channel data.

	def Preserve_Channel(self):
		if self.channel != '':
			self.flag = True
		#	print(self.flag)
		self.final_posts = []
		self.posts_lists = []
		Posts = """select CreateAt from Posts where ChannelId = %s"""
		lh = len(self.channels)
		for m in range(lh):
			posts_data = ''.join(self.channels[m])
			self.cursor.execute(Posts, (posts_data,))
			posts_out = self.cursor.fetchall()
			for out in posts_out:
				posts_output = ''.join(str(out))
				posts_output =  posts_output.replace(',)', '')
				posts_output =  posts_output.replace('(', '')
				self.posts_lists.append(posts_output)
		self.final_posts= list(set(self.Final_Date_Lists) - set(self.posts_lists))
		#print(self.final_posts)
		print("Finding Posts to be deleted is [OK]")

#####################################################################################################################################################################
#In this part of provide disk quota for mattermost user. 

	def Disk_Quota(self):
		user_id_lists = []
		path_details_lists = []
		size_path_dict = {}
		USER_ID = """select Id from Users"""
		FILE_PATH = """select Path from FileInfo where CreatorId = %s"""
		FILE_DELETE_QUERY = """select Path from FileInfo where DeleteAt > 0  """
		self.cursor.execute(FILE_DELETE_QUERY)
		del_file = self.cursor.fetchall()
		for file_status in del_file:
			file_status = ''.join(file_status)
			del_file_path = "/opt/mattermost/data/" + file_status
			file_deletion_cmd = "rm -rf " +  del_file_path[0:138]
			os.system(file_deletion_cmd)
		self.cursor.execute(USER_ID)
		id_details = self.cursor.fetchall()
		for ids in id_details:
			ids = ''.join(ids)
			user_id_lists.append(ids)
		user_id_lists_len = len(user_id_lists)
		for id_size in range(user_id_lists_len):
			self.cursor.execute(FILE_PATH, (user_id_lists[id_size],))
			path_info = self.cursor.fetchall()
			for path_details in path_info:
				path_details = ''.join(path_details)
				complete_path = "/opt/mattermost/data/" + path_details
                                #print(complete_path[0:84])
				total_size = subprocess.check_output(['du','-sb', complete_path[0:112]]).split()[0].decode('utf-8')
		
				size_path_dict[complete_path[0:84]] = total_size
		for key,value in size_path_dict.items():
			if int(value) > 20000000:
				#print(value)
				os.chdir(key)
				os.system('chmod 555 *')
			else:
				os.chdir(key)
				os.system('chmod 750 *')
		self.connection.commit()
#####################################################################################################################################################################
#This part of script create audit logs for mattermost.

	
	def Log_Collector(self):
		log_collection = []
		id_collection_list = []
		name_collection_list = []
		id_name_dict = {}
		data_tmp_list = []
		data_actual_list = []
		USER_NAME_QUERY = """select Username from Users where Id = %s"""
		self.cursor.execute('select Id from Users')
		log_name = self.cursor.fetchall()
		for name_Id in log_name:
			name_Id = "".join(name_Id)
			id_collection_list.append(name_Id)
		id_len = len(id_collection_list)
		for loop in range(id_len):
			self.cursor.execute(USER_NAME_QUERY, (id_collection_list[loop],))
			user_name = self.cursor.fetchall()
			for user_info in user_name:
				user_info = ''.join(user_info)
				id_name_dict[id_collection_list[loop]] = user_info
                                #print(id_name_dict)
                #for len_num in range(id_len):
                #       id_name_dict[id_collection_list[len_num]] = name_collection_list[len_num]
                #print(id_name_dic
		id_name_dict_len = len(id_name_dict)
		self.cursor.execute('select * from Audits')
		log_data = self.cursor.fetchall()
		for logs in log_data:
			logs = "".join(str(logs))
			log_collection.append(logs)
			actual_logs = list(dict.fromkeys(log_collection))
			log_collection_len = len(actual_logs)
		for log_data in range(log_collection_len):
			readable_time=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(actual_logs[log_data][31:41])))
			actual_date = actual_logs[log_data]
			replaced_date = actual_date[31:41]
			readable_date = actual_date.replace(replaced_date, str(readable_time))
			data_tmp_list.append(readable_date)
		with open('/opt/mattermost/logs/mattermost_audit.logs', 'r') as actual_content:
			actual_in = actual_content.readlines()
			for data_actual in actual_in:
				data_actual_list.append(data_actual)
		final_logs= list(set(data_tmp_list) - set(data_actual_list))
		final_logs_len = len(final_logs)
		with open('/opt/mattermost/logs/mattermost.log', 'w') as matter_content:
			for matter_data in range(final_logs_len):
				written_data = ''.join(final_logs[matter_data])
				matter_content.write(written_data)
				matter_content.write('\n')
		for str_name, str_replace in id_name_dict.items() :
			replace_string_cmd = "sed -i 's/" + str(str_name) + "/" + str(str_replace) + "/g' /opt/mattermost/logs/mattermost.log"
			os.system(replace_string_cmd)
			os.system('rm -rf /opt/mattermost/logs/sed*')				
		self.connection.commit()	
					
##########################################################################################################################################################
##In this part of script we have to delete file info from database as well as from mattermost data directory.
				 

	def File_Info(self):			
		self.File_Path_Lists = []
		PATH = """select Path from FileInfo where CreateAt = %s"""
		THUMBNAIL = """select ThumbnailPath from FileInfo where CreateAt = %s"""
		PREVIEW = """select PreviewPath from FileInfo where CreateAt = %s"""
		for file_date in self.final_posts:
			file_date=file_date.rstrip("\n")
			file_date=str(file_date)
			for rem_file in PATH,THUMBNAIL,PREVIEW:
				self.cursor.execute(rem_file, (file_date,))
				files = self.cursor.fetchall()
				for x in files:
					x = ''.join(x)
					del_data = ''.join(self.channels)
					x_len = len(x)
					if x_len == 0:
						break
					if x[31:57] == del_data:
						self.File_Path_Lists.append(x)
					if x[31:57] != del_data:
						CMD = "rm -rf /opt/mattermost/data/" + x[0:57]
				#		print(CMD)
						os.system(CMD)		
		print("Fiding file path to delete from storage is [OK]")


####################################################################################################################################################################
#In this part of script we have preserve file info for the channel which we don't want to be deleted.

	def Preserve_File_Info(self):
		File_Time_Lists = []	
		FILE_QUERY = """select CreateAt from FileInfo where Path = %s """
		for file_details in self.File_Path_Lists:
			file_details=file_details.rstrip("\n")
			file_details=str(file_details)
			self.cursor.execute(FILE_QUERY, (file_details,))
			files_info = self.cursor.fetchall()
			for info in files_info:
#				posts_output = ''.join(str(out))
				info_out = ''.join(str(info))
				info_out =  info_out.replace(',)', '')
				info_out =  info_out.replace('(', '')
				File_Time_Lists.append(info_out)
		self.Final_Date = list(set(self.final_posts) - set(File_Time_Lists))
#		print(self.Final_Date)
		print("Finding Channel Data which should preserve is [OK]")
################################################################################################################################################################
#This part of the script delete posts and file info from mattermost.



	def File_Del(self):
		if self.flag == False:
			DELPOSTS = """delete from Posts where CreateAt = %s"""
			DELFILE = """delete from FileInfo where CreateAt = %s"""
			for del_posts in self.Final_Date_Lists:
				del_posts=del_posts.rstrip("\n")
				del_posts=str(del_posts)
				self.cursor.execute(DELPOSTS, (del_posts,))
				self.connection.commit()
				self.cursor.execute(DELFILE, (del_posts,))
				self.connection.commit()					
				time.sleep(2)
			print("Files are removed from Database and File Storage successfully")

		else:
                        DELPOSTS = """delete from Posts where CreateAt = %s"""
                        DELFILE = """delete from FileInfo where CreateAt = %s"""
                        for del_posts in self.Final_Date:
                                del_posts=del_posts.rstrip("\n")
                                del_posts=str(del_posts)
                                self.cursor.execute(DELPOSTS, (del_posts,))
                                self.connection.commit()
                                self.cursor.execute(DELFILE, (del_posts,))
                                self.connection.commit()
                                time.sleep(2)
                        print("Files are removed from Database and File Storage successfully")
			
#			
call = Data_Retention(False)

'''call.db()
call.Date_Query()
call.Channel_Id()
call.Preserve_Channel()
call.File_Info()
call.Preserve_File_Info()
call.File_Del()'''


def main(till_date):
	try:
		time.sleep(5)
		today_date = subprocess.check_output(['date  --date="0 day ago"  "+%s"'], shell=True).split()[0].decode('utf-8')	
		#print(today_date)
		#print(till_date)
		call.db()
		call.Date_Query()
		call.Channel_Id()
		call.Preserve_Channel()
		call.File_Info()
		call.Preserve_File_Info()
		call.File_Del()
		while True:
			call.Log_Collector()
			call.Disk_Quota()
			if today_date[5] == till_date[5]:
				break
		next_date = subprocess.check_output(['date  --date="-1 day ago"  "+%s"'], shell=True).split()[0].decode('utf-8')
		main(next_date)
		
	except:
		next_date = subprocess.check_output(['date  --date="-1 day ago"  "+%s"'], shell=True).split()[0].decode('utf-8')
		main(next_date)
if __name__ == '__main__':
	next_date = subprocess.check_output(['date  --date="-1 day ago"  "+%s"'], shell=True).split()[0].decode('utf-8')
	main(next_date)
	
