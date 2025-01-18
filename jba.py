import os
import sys
import time
import datetime
from dateutil.relativedelta import relativedelta
import mysql.connector
import numpy as np

FILE_PATH = "/home/ubuntu/Documents/jba_code/JBA data/"
ext = ".pre"

def date_inc(yr,mnt):
	year = yr
	month = 1
	day = 1
	mydatef = datetime.datetime(year,day,month)
	if mnt == 0:
		return mydatef.strftime("%Y-%d-%m")
	else:
		mydatef += relativedelta(months=mnt)
		
		return mydatef.strftime("%Y-%d-%m")
def dbconn(data):
	
	try:
		con = mysql.connector.connect(host='localhost',user='root',password="root", database='rainfalldb')
		cur = con.cursor()
		cur.execute("SELECT VERSION()")
		result = cur.fetchone()
		insert_data(cur,con,data)
		
	except mysql.connector.Error as e:
		con.rollback()
		
		print (e)
	
	con.close()
		

def create_table(cur):
	cur.execute("DROP TABLE IF EXISTS rain_info")
	sql = """ CREATE TABLE rain_info (Xref int, Yref int, date varchar(45), Value int) """
	cur.execute(sql)

def insert_data(cur,con,data):
	xref = int(data[0])
	yref = int(data[1])
	xdate = str(data[2])
	value = int(data[3])
	
	cur.execute(" INSERT INTO `rainfalldb`.`rain_info` (`Xref`,`Yref`,`date`,`value`) VALUES (%s,%s,%s,%s)",(xref,yref,xdate,value))
	con.commit()
	cur.close()
	con.close()

def main():
	try:
		if not os.path.exists(FILE_PATH):
			print ("Wrong file path {}".format())
			sys.exit(0)
		for dirpath, dirname, files in os.walk(FILE_PATH):
			for filename in files:
				if filename.lower().endswith(ext):
					with open(os.path.join(dirpath,filename),"r") as rain_data:
						rain_data.readline()
						each_year = []
						testarray = []
						for i,rows in enumerate (rain_data):
							
							if i >= 4:
								if "Grid-ref" in rows:
									yr = 1991
									rows = rows.replace("Grid-ref=","")
									Xref,Yref = rows.split(",")
									Yref = Yref.replace("\n","")

								else:
									count = 0
									values = []
									xref = []
									yref = []
									year_months = [] 
									
									for column in rows.split():
										
										xref.append(int(Xref))
										yref.append(int(Yref))
										year_months.append(date_inc(yr,count))
										values.append(int(column))
										testarray.append(int(Xref))
										testarray.append(int(Yref))
										testarray.append(date_inc(yr,count))
										testarray.append(int(column))
										
										dbconn(testarray)
										time.sleep(1)"""The sleep time is added to check the database transaction"""
										testarray = []
										count += 1
										if count == 12:
											yr += 1

								rain_trans = np.transpose(testarray)
								rain_trans_list = (rain_trans).tolist()
								testarray = []
								each_year = []

 
	except () as err:
		print "Unexpected error",err
if __name__ == "__main__":
	print "==================================="
	print "==                               =="
	print "==        Code is writing        =="
	print "==              by               =="
	print "==           Babatunde           =="
	print "==          24/July/2018         =="
	print "==================================="
	print "==================================="
	try:
		main()
		#date_inc()
		#dbconn()
	except(KeyboardInterrupt):
		print "Program Interrupted Ctl+c"
		sys.exit(0)
