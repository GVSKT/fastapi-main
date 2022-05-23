from fastapi import FastAPI,Response,Request,status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from typing import Optional
import mysql.connector
import json
from utils.progress import get_host
from pydantic import BaseModel
from config.config import db_config
from collections import ChainMap
from decimal import *
import datetime
from datetime import timedelta,date,datetime
import uvicorn
#import gunicorn
#import uvicorn.workers.UvicornWorker

app=FastAPI()

@app.get('/KronosApi/Emp_Repeat_Offender/')
async def Emp_Repeat_Offender():
    try:
        host, user, pswd, db = db_config()
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        #db=mysql.connector.connect(user=user, password= pswd, host= host, database= db,auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        print("\nEmp_Repeat_Offender Api Started")
        cursor.execute("SELECT * FROM kronos.emp_repeat_offender ORDER BY cumulative_missing_swipes ")
        columns = cursor.description
        results = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        result={"employee_report_card":results}
        final_respose=jsonable_encoder(result)
        #return JSONResponse(content=final_respose)
        respone=JSONResponse(content=final_respose)
        respone.status_code = 200
        return respone
        print("\nEmp_Repeat_Offender Api Ended!!!")
        #return status.JSONResponse(content=final_respose)

    except mysql.connector.Error as ex:
        print("\nError At Emp_Data", ex)


@app.post('/KronosApi/Emp_Repeat_Offender_Page_Nation/')
async def Emp_Repeat_Offender_Page_Nation(request:Request):
    try:
        host, user, pswd, db = db_config()
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        #db=mysql.connector.connect(user=user, password= pswd, host= host, database= db,auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        final_result=[]
        page_sel= await request.json()
        print("\npage_sel : ",page_sel)
        page_select = int(page_sel['pageno'])
        
        print("\n----------------------------Emp_Repeat_Offender_Page_Nation Api Started-------------------------")
        
        if page_select==1:
            page_final_select=int(1)
            id_select=int(page_select*10)
        else:
            id_select=int(page_select*10)
            page_final_select=int(id_select-9)
            
        print("\npage_final_select & id_select : ",page_final_select,' & ',id_select)
        cursor.execute("SELECT * FROM kronos.emp_repeat_offender  WHERE id BETWEEN %s AND %s\
        ORDER BY cumulative_missing_swipes  ",(page_final_select,id_select) )
        columns = cursor.description
        results = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        final_result=results
        
        cursor.execute("SELECT COUNT(*) AS total_records FROM kronos.emp_repeat_offender")
        columns = cursor.description
        count_list_result = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        value={key:i[key] for i in count_list_result for key in count_list_result[0]}
        #value={res[key].append(sub[key]) for sub in count_list_result for key in sub}
        print(value)
        
        list_result={"data":final_result}
        list_result.update(page_sel)
        list_result.update(value)
        final_respose=jsonable_encoder(list_result)
        return JSONResponse(content=final_respose)
        
        print("\n----------------------------Emp_Repeat_Offender_Page_Nation Api Ended-------------------------")

    except mysql.connector.Error as ex:
        print("\nError At Emp_Repeat_Offender_Page_Nation", ex)



@app.post('/KronosApi/Employee_Repeat_Offender_Pay_Period_Wise/')
async def Employee_Repeat_Offender_Pay_Period_Wise(request:Request):
    try:
        host, user, pswd, db = db_config()
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        #db=mysql.connector.connect(user=user, password= pswd, host= host, database= db,auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        requested_date_select= await request.json()
        date_select = requested_date_select['date']
        print("Selected Pay Period End  Date is : ",date_select)
        
        cursor.execute("CALL sp_employees_payperiod_missed_punch_data(%s)",(date_select,))
        columns = cursor.description
        results = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        result={"employee_report_card":results}
        final_respose=jsonable_encoder(result)
        return JSONResponse(content=final_respose)
        print("\nEmployee_Repeat_Offender_Pay_Period_Wise Api Ended!!!")
        
        
    except mysql.connector.Error as ex:
        print("\nError At Emp_Data", ex)

@app.post('/KronosApi/Manager_Repeat_Offender/')
async def Manager_Repeat_Offender(request:Request):
    try:
        host, user, pswd, db = db_config()
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        #db=mysql.connector.connect(user=user, password= pswd, host= host, database= db,auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        requested_date_select= await request.json()
        date_select = requested_date_select['date']
        print("Selected Pay Period Date is : ",date_select)
        print("\nManager_Repeat_Offender Api Started")
        
        cursor.execute("SELECT DATE_FORMAT(STR_TO_DATE(pay_period_biweekly,'%m/%d/%Y'),'%Y-%m-%d') FROM kronos.previous_pay_period\
        WHERE pay_period_end_date=%s",(date_select,))
        pay_period_date_=cursor.fetchone()
        print("Pay Period Date is : ",pay_period_date_)
        
        cursor.execute("SELECT manager_id,manager_name,depthd_id,depthd_name,number_of_unapproved_time_cards_previos_pay_period,total_no_of_staff_per_manager,\
        round(unapproved_timecards_percentage_per_manager) AS unapproved_timecards_percentage_per_manager,round(unapproved_avg_ytd) AS unapproved_avg_ytd \
        FROM kronos.manager_repeat_offender WHERE pay_period_date_format=%s\
        ORDER BY unapproved_timecards_percentage_per_manager ASC ",pay_period_date_) 
        
        columns = cursor.description 
        results = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        results={"manager_report_card":results}
        final_respose=jsonable_encoder(results)
        return JSONResponse(content=final_respose)
        print("\nManager_Repeat_Offender Api Ended!!!!")
        
        
    except mysql.connector.Error as ex:
        print("\nError At Emp_Data", ex)

@app.get('/KronosApi/Manager_Repeat_Offender_Excel_Generation/')
async def Manager_Repeat_Offender_Excel_Generation():
    try:
        host, user, pswd, db = db_config()
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        #db=mysql.connector.connect(user=user, password= pswd, host= host, database= db,auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        cursor.execute("SELECT DISTINCT(DATE_FORMAT(pay_period_date_format,'%Y-%m-%d')) FROM  kronos.manager_repeat_offender ORDER BY pay_period_date_format DESC")
        pay_date=cursor.fetchall()
        cumulative_data=[]
        for dates in pay_date:
            #print(dates)
            cursor.execute("SELECT * FROM kronos.manager_repeat_offender\
            WHERE  pay_period_date_format=%s ORDER BY unapproved_timecards_percentage_per_manager ASC ",dates)
            columns = cursor.description 
            results = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
            res={"pay_period_data":results}
            res.update({"pay_period_date":dates[0]})
            cumulative_data.append(res)
        final={"manager_report_card":cumulative_data}
        final_respose=jsonable_encoder(final)
        return JSONResponse(content=final_respose)
        print("\n---------------------------  Manager_Repeat_Offender_Excel_Generation Api Ended --------------")
        
        
    except mysql.connector.Error as ex:
        print("\nError At Emp_Data", ex)

@app.get('/KronosApi/Missing_Swipes_by_week/')
async def Missing_Swipes_by_week():
    try:
        host, user, pswd, db = db_config()
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        #db=mysql.connector.connect(user=user, password= pswd, host= host, database= db,auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        print("\nMissing_Swipes_by_week Api Started")
        
        #''' Incase Missing_Swipes_by_week Table is Truncated Run this part to insert complete data starting from PRD '''
        #"""START
        """cursor=db.cursor()
        today = date.today()
        cursor.execute("SELECT saturday_week,missed_swipes_week_count FROM kronos.missed_swipes_by_week")
        datas=[i[0] for i in cursor.fetchall()]
        print(datas)
        for data in datas:
            print("\nDATA", data)
            current_date = datetime.strptime(data, '%m/%d/%Y')
            Prev_Date=datetime.strftime(current_date - timedelta(6), '%m/%d/%Y')
            #val=(Prev_Date,current_date)
            val=(Prev_Date,data)
            sql_fetch="SELECT COUNT(emp_id) FROM kronos.missed_swipes_report  WHERE miss_punch_date BETWEEN %s AND %s"
            db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
            cursor=db.cursor()
            cursor.execute(sql_fetch,val)
            datas_count=cursor.fetchone()
            data_count=','.join(map(str,datas_count))
            print(data_count)
            val_update=data_count,data
            print("val_update",val_update)
            
            db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.140', database= 'kronos',auth_plugin = 'mysql_native_password')
            cursor=db.cursor()
            cursor.execute(" UPDATE kronos.missed_swipes_by_week SET missed_swipes_week_count=%s WHERE saturday_week=%s",val_update)
            db.commit()"""
        #END"""
        
        cursor.execute("SELECT * FROM kronos.missed_swipes_by_week")
        columns = cursor.description
        results = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        print(results)
        cursor.execute("SELECT max(missed_swipes_week_count) as max_value, min(missed_swipes_week_count) as min_value FROM kronos.missed_swipes_by_week")
        columns = cursor.description
        result_new = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        
        result={"missing_swipes_by_week":results,"min_value":result_new[0]['min_value'],'max_value':result_new[0]['max_value']}
        final_respose=jsonable_encoder(result)
        return JSONResponse(content=final_respose)
    
        print("\n---------- Missing_Swipes_by_weekApi Ended -----------------")
        db.close()
        
    except mysql.connector.Error as ex:
        print("\nError At Emp_Data", ex)
        db.close()

@app.get('/KronosApi/Unapproved_Time_Cards_By_Pay_Period/')
async def Unapproved_Time_Cards_By_Pay_Period():
    try:
        host, user, pswd, db = db_config()
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        #db=mysql.connector.connect(user=user, password= pswd, host= host, database= db,auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        """print("\nApi Started")
        cursor.execute("SELECT * FROM kronos.previous_pay_period")
        datas=[i[1] for i in cursor.fetchall()]
        print("\nDATAS",datas)
        for data in datas:
            print("\nPay Period Date is:  " ,data)
            
            sql_select="SELECT DISTINCT(dirmgr_email),created_date FROM kronos.biweekly_missed_swipes_report WHERE  periodic_count=1 AND created_date =%s"
            #sql_select="SELECT COUNT(emp_id),created_date FROM kronos.biweekly_missed_swipes_report WHERE  periodic_count=1 AND created_date =%s"
            cursor.execute(sql_select,(data,))
            sel_data=[i[0] for i in cursor.fetchall()]
            #final_count=', '.join(map(str,sel_data))
            final_count=len(sel_data)
            print("\nCount of Unapproved Time Cards",final_count)
            
            val_update=(final_count,data)
            sql_update="UPDATE kronos.previous_pay_period SET unapproved_time_cards_count=%s WHERE pay_period_biweekly=%s"
            cursor.execute(sql_update,val_update)
            db.commit()"""
        
        cursor.execute("SELECT percent_mgr_unapp_TC as percent_mgr_not_approve_TC, pay_period_end_date AS pay_period_biweekly,unapproved_time_cards_count, \
        CONVERT(unappr_TC_Pay_Period_Perc,CHAR) AS unappr_TC_Pay_Period_Perc FROM kronos.previous_pay_period ")
        columns = cursor.description 
        results = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        result={"unapproved_time_cards_by_pay_period":results}
        final_respose=jsonable_encoder(result)
        return JSONResponse(content=final_respose)
        print("\n----------------------------- Unapproved_Time_Cards_By_Pay_Period API Ended ----------------------")
        db.close()
        
    except mysql.connector.Error as ex:
        print("\nError At Emp_Data", ex)
        db.close()

@app.post('/KronosApi/emp_weekly_stats/')
async def emp_weekly_stats(request:Request):
    db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
    cursor=db.cursor()
    
    print("\nIn emp_weekly_stats")
    requested_date_select= await request.json()
    date_select = requested_date_select['date']
    #date_select = "2021-11-14"
   
    try:
        cursor.execute("SELECT dayname as day, miss_punch_date, percentage_sum_count ,sum_count,total_employees_count\
                       FROM kronos.daily_employees_count where week_date =  %s order by id asc;",(date_select,))
        columns = cursor.description 
        result = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        #print(result)
        week_sum = 0
        for i in range(0,7):
            #print(result[i]['sum_count'])
            week_sum += int(result[i]['sum_count'])
            
        cursor.execute("SELECT sum(total_employees_count) as total_sum\
                       FROM kronos.daily_employees_count where week_date =  %s ;",(date_select,))
        columns = cursor.description 
        result1 = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        res = result1[0]['total_sum'] 
        #print(type(result1))
        
        final_check3 = {"weekly_miss":week_sum,"weekly_missed_swip_percentage":round(((week_sum)*100)/res,2),"weekdata": result,"weekly_total_employees_count":int(res)}
        final_respose=jsonable_encoder(final_check3)
        return JSONResponse(content=final_respose)
        print("\n----------------------------- emp_weekly_stats API Ended ----------------------")
        
        
    
    except:
        r =  {
        "weekdata": [
        {"day": 0,"miss_punch_date": 0,"sum_count":0,"percentage_sum_count": 0},
        {"day": 0,"miss_punch_date": 0,"sum_count":0,"percentage_sum_count": 0},
        {"day": 0,"miss_punch_date": 0,"sum_count":0,"percentage_sum_count": 0},
        {"day": 0,"miss_punch_date": 0,"sum_count":0,"percentage_sum_count": 0},
        {"day": 0,"miss_punch_date": 0,"sum_count":0,"percentage_sum_count": 0},
        {"day": 0,"miss_punch_date": 0,"sum_count":0,"percentage_sum_count": 0},
        {"day": 0,"miss_punch_date": 0,"sum_count":0,"percentage_sum_count": 0}],
        "weekly_miss": 0,
        "weekly_missed_swip_percentage": 0
        }
        final_respose=jsonable_encoder(r)
        return JSONResponse(content=final_respose)


@app.get('/KronosApi/manager_pay_period_status_all_data/')
async def manager_pay_period_status_all_data():
    try:
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        #Here "pay_period_biweekly " is given as "pay_period_date".Now we are giving the "pay_period_end_date" as "pay_period_date" 
        cursor.execute("select pay_period_end_date as pay_period_date,ytd_perc_mgr_uapp_new as cum_sum_mgr_not_approve_TC ,\
                        percent_mgr_unapp_TC as \
                       percent_mgr_not_approve_TC ,Total_no_of_unapp_TC \
                        as total_no_of_unapproved_TC_per_pay_period, mgr_approved_TC as mgr_approve_TC,\
                        total_managers_count_per_pay_Period as total_managers_count_per_pay_period,total_managers_count_per_pay_Period - mgr_approved_TC \
                        as mgr_not_approve_TC from \
                        kronos.previous_pay_period where pay_period_biweekly is not null\
                        order by id desc limit 26;")
                       
        columns = cursor.description 
        a = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()] 
        print(a)
        final_manager = {"pay_period_list":a}
        final_respose=jsonable_encoder(final_manager)
        return JSONResponse(content=final_respose)

    except mysql.connector.Error as ex:
            print("\nError At Emp_Data", ex)


@app.get('/KronosApi/emp_weekly_stats_all_data/')
async def emp_weekly_stats_all_data():
    try:
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        print("\nIn EMP_DATA")
        
        cursor.execute("select week_Date,dayname as day,miss_punch_date,percentage_sum_count,sum_count,total_employees_count from \
                         kronos.daily_employees_count where week_Date != 0  order by \
                         id desc limit 365;")

        columns = cursor.description 
        result = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        #print(result[0])
        cursor.execute("select total_employees_count from \
                         kronos.daily_employees_count where week_date != 0 \
                         order by miss_punch_date desc limit 365;")
        columns = cursor.description 
        result1 = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]

    #     #print(result)
        a = []
        week_miss = []
        total_emp = []
        for i in range (0,len(result),7):
            b= (result[i+6],result[i+5],result[i+4],result[i+3],result[i+2],result[i+1],result[i])
            #print(b)
            a.append(b)
            week_ =  int(result[i+6]['sum_count'])+int(result[i+5]['sum_count'])+int(result[i+4]['sum_count'])+int(result[i+3]['sum_count'])+int(result[i+2]['sum_count'])+int(result[i+1]['sum_count'])+int(result[i]['sum_count']) 

            totat_emp_ =  int(result1[i+6]['total_employees_count'])+int(result1[i+5]['total_employees_count'])+int(result1[i+4]['total_employees_count'])+int(result1[i+3]['total_employees_count'])+int(result1[i+2]['total_employees_count'])+int(result1[i+1]['total_employees_count'])+int(result1[i]['total_employees_count'])
            week_miss.append(week_)
            total_emp.append(totat_emp_)
        pecent_weekend = []
        for i in range(len(total_emp)):
            q = round(week_miss[i]*100/total_emp[i],2)
            pecent_weekend.append(q)
        final = []
        for i in range(len(total_emp)):
            w = {"weekly_miss":week_miss[i],"weekly_missed_swip_percentage":round(week_miss[i]*100/total_emp[i],2),"weekdata":list(a[i]),"weekly_total_employees_count":total_emp[i]}
            final.append(w)
            #print(w)

        final_new = {"emp_weekly_all_stats_data":final}
        final_respose=jsonable_encoder(final_new)
        return JSONResponse(content=final_respose)
        
        
    except mysql.connector.Error as ex:
        print("\nError At Emp_Data", ex)


@app.post('/KronosApi/emp_weekly_excel/')
async def emp_weekly_excel(request:Request):
    try:
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        print("\nIn EMP_DATA")
        requested_date_select= await request.json()
        to_date_select = requested_date_select['to_date']
        

        cursor.execute("select week_Date,dayname as day,miss_punch_date,percentage_sum_count,sum_count,total_employees_count from \
                         kronos.daily_employees_count where week_Date = %s  order by id desc ;",(to_date_select,))

        columns = cursor.description 
        result = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        #print(result[0])
        cursor.execute("select total_employees_count from \
                         kronos.daily_employees_count where week_date = %s \
                         order by miss_punch_date desc;",(to_date_select,))
        columns = cursor.description 
        result1 = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]

    #     #print(result)
        a = []
        week_miss = []
        total_emp = []
        for i in range (0,len(result),7):
            b= (result[i+6],result[i+5],result[i+4],result[i+3],result[i+2],result[i+1],result[i])
            a.append(b)
            week_ =  int(result[i+6]['sum_count'])+int(result[i+5]['sum_count'])+int(result[i+4]['sum_count'])+int(result[i+3]['sum_count'])+int(result[i+2]['sum_count'])+int(result[i+1]['sum_count'])+int(result[i]['sum_count']) 

            totat_emp_ =  int(result1[i+6]['total_employees_count'])+int(result1[i+5]['total_employees_count'])+int(result1[i+4]['total_employees_count'])+int(result1[i+3]['total_employees_count'])+int(result1[i+2]['total_employees_count'])+int(result1[i+1]['total_employees_count'])+int(result1[i]['total_employees_count'])
            week_miss.append(week_)
            total_emp.append(totat_emp_)
        pecent_weekend = []
              
        for i in range(len(total_emp)):
            q = round(week_miss[i]*100/total_emp[i],2)
            pecent_weekend.append(q)
        final = []
              
        for i in range(len(total_emp)):
            w = {"weekly_miss":week_miss[i],"weekly_missed_swip_percentage":round(week_miss[i]*100/total_emp[i],2),"weekdata":list(a[i]),"weekly_total_employees_count":total_emp[i]}
            final.append(w)

        final_new = {"emp_weekly_excel":final}
        final_respose=jsonable_encoder(final_new)
        return JSONResponse(content=final_respose)
        

    except mysql.connector.Error as ex:
        print("\nError At Emp_Data", ex)


@app.post('/KronosApi/pay_period_end_dates_list/')
async def pay_period_end_dates_list(request:Request):
    
    try:
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        requested_date_select= await request.json()
              
        from_date_select = requested_date_select['from_date']
        to_date_select = requested_date_select['to_date']
         
        if from_date_select != '' and to_date_select != '':
            cursor.execute("SELECT distinct(pay_period_end_date) FROM kronos.previous_pay_period WHERE STR_TO_DATE(pay_period_end_date ,'%m/%d/%Y')>=STR_TO_DATE(%s,'%m/%d/%Y')\
                        and STR_TO_DATE(pay_period_end_date ,'%m/%d/%Y')<=STR_TO_DATE(%s,'%m/%d/%Y') order by id desc;",(from_date_select,to_date_select))
                       
            columns = cursor.description 
            a = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()] 
            print(a)
            final_dates = {"pay_period_end_dates_list":a}
            final_respose=jsonable_encoder(final_dates)
            return JSONResponse(content=final_respose)
            
        
    except mysql.connector.Error as ex:
        print("\nError At pay_period_list", ex)

        
@app.post('/KronosApi/daily_week_list/')
async def manager_pay_period_daily_week_list(request:Request):
    
    try:
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        requested_date_select= await request.json()
              
        from_date_select = requested_date_select['from_date']
        to_date_select = requested_date_select['to_date']
         
        if from_date_select != '' and to_date_select != '':
            cursor.execute("SELECT distinct(week_date) FROM kronos.daily_employees_count WHERE STR_TO_DATE(week_date ,'%m/%d/%Y')>=STR_TO_DATE(%s,'%m/%d/%Y')\
                        and STR_TO_DATE(week_date ,'%m/%d/%Y')<=STR_TO_DATE(%s,'%m/%d/%Y') order by id desc;",(from_date_select,to_date_select))
                       
            columns = cursor.description 
            a = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()] 
            print(a)
            final_manager = {"pay_period_list":a}
            final_respose=jsonable_encoder(final_manager)
            return JSONResponse(content=final_respose)
        else:
            cursor.execute("SELECT week_date FROM kronos.daily_employees_count order by STR_TO_DATE(week_date ,'%m/%d/%Y') desc limit 1;")
            columns = cursor.description 
            a = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()] 
            print(a)
            final_manager = {"pay_period_list":a}
            final_respose=jsonable_encoder(final_manager)
            return JSONResponse(content=final_respose)
            

    except mysql.connector.Error as ex:
            print("\nError At Emp_Data", ex)

@app.post('/KronosApi/Daily_Missing_Employees_Date_Ranges/')
async def Daily_Missing_Employees_Date_Ranges(request:Request):
    try:
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        requested_date_select= await request.json()
              
        usage=requested_date_select['period']
        from_date_select = requested_date_select['from_date']
        to_date_select = requested_date_select['to_date']

        if usage=='daily':
            if from_date_select!='' and  to_date_select!='':
                print("\nRequested Daily With Date Range")
                FROM_DATE=datetime.strptime(from_date_select,'%m/%d/%Y').date()
                TO_DATE=datetime.strptime(to_date_select,'%m/%d/%Y').date()
                
                cursor.execute("SELECT DISTINCT(miss_punch_date) AS missed_date FROM  daily_missing_employees_hris WHERE miss_punch_date_format \
                       BETWEEN %s AND %s  ORDER BY miss_punch_date_format  DESC ", (FROM_DATE,TO_DATE) )
                
                columns = cursor.description 
                results = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
                result={"missed_date_ranges":results}
                final_respose=jsonable_encoder(result)
                return JSONResponse(content=final_respose)
                print("\n---------- Daily_Missing_Employees_Date_Ranges Api Ended ----------------------------")
                db.close()
            
            else:
                print("\nRequested Daily Without Date Range.Hence Generating 1 Week Date Ranges")
                FROM_DATE=datetime.strftime(datetime.now()-timedelta(8),'%Y-%m-%d')
                TO_DATE=datetime.strftime(datetime.now(),'%Y-%m-%d')
                cursor.execute("SELECT DISTINCT(miss_punch_date) AS missed_date FROM daily_missing_employees_hris WHERE miss_punch_date_format\
                BETWEEN %s AND %s  ORDER BY miss_punch_date_format  DESC  ", (FROM_DATE,TO_DATE) )
                columns = cursor.description 
                results = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
                result={"missed_date_ranges":results}
                final_respose=jsonable_encoder(result)
                return JSONResponse(content=final_respose)
                print("\n---------- Daily_Missing_Employees_Date_Ranges Api Ended ----------------------------")
                db.close()
                
        elif usage=='biweekly':
            if from_date_select!='' and  to_date_select!='':
                print("\nRequested Bi-Weekly With Date Range.")
                FROM_DATE=datetime.strptime(from_date_select,'%m/%d/%Y').date()
                TO_DATE=datetime.strptime(to_date_select,'%m/%d/%Y').date()
                
                cursor.execute("SELECT DISTINCT(pay_period_date) AS missed_date FROM  missing_employees_hris WHERE pay_period_date_format \
                       BETWEEN %s AND %s  ORDER BY pay_period_date_format  DESC ", (FROM_DATE,TO_DATE) )
                
                columns = cursor.description 
                results = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
                result={"missed_date_ranges":results}
                final_respose=jsonable_encoder(result)
                return JSONResponse(content=final_respose)
                print("\n---------- Daily_Missing_Employees_Date_Ranges Api Ended ----------------------------")
                db.close()
            
            else:
                print("\nRequested Bi-Weekly Without Date Range.Hence Generating 1 Week Date Ranges")
                FROM_DATE=datetime.strftime(datetime.now()-timedelta(15),'%Y-%m-%d')
                TO_DATE=datetime.strftime(datetime.now(),'%Y-%m-%d')
                cursor.execute("SELECT DISTINCT(pay_period_date) AS missed_date FROM missing_employees_hris WHERE pay_period_date_format\
                BETWEEN %s AND %s  ORDER BY pay_period_date_format  DESC  ", (FROM_DATE,TO_DATE) )
                columns = cursor.description 
                results = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
                result={"missed_date_ranges":results}
                final_respose=jsonable_encoder(result)
                return JSONResponse(content=final_respose)
                print("\n---------- Daily_Missing_Employees_Date_Ranges Api Ended ----------------------------")
                db.close()
                
        else:
            print("Requested Neither Daily Nor Weeekly")
            
            
    except mysql.connector.Error as ex:
        print("\nError At Emp_Data", ex)
        


@app.post('/KronosApi/missing_employees_hris/')
async def missing_employees_hris(request:Request):
    db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
    cursor=db.cursor()
    requested_date_select= await request.json()
              
    usage=requested_date_select['period']
    date_select = requested_date_select['date']
    
    try:
        if usage=='daily':
            print("\nRequested For Daily Employees Missing List ")
            Required_Date=datetime.strptime(date_select,'%m/%d/%Y').date()
            cursor.execute("SELECT DISTINCT emp_id,employee_name,DATE_FORMAT(miss_punch_date_format,'%Y-%m-%d') AS missed_date\
            FROM daily_missing_employees_hris WHERE miss_punch_date_format=%s ",(Required_Date,))
            columns = cursor.description 
            result = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
            results={"missed_employees_hris":result}
            final_respose=jsonable_encoder(results)
            return JSONResponse(content=final_respose)
        
        elif usage=='biweekly':
            print("\nRequested For Bi-Weekly Employees Missing List ")
            Required_Date=datetime.strptime(date_select,'%m/%d/%Y').date()
            cursor.execute("SELECT DISTINCT employe_name,employe_id,DATE_FORMAT(pay_period_date_format,'%Y-%m-%d') AS missed_date\
            FROM missing_employees_hris WHERE pay_period_date_format=%s",(Required_Date,))
            columns = cursor.description 
            result = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
            results={"missed_employees_hris":result}
            final_respose=jsonable_encoder(results)
            return JSONResponse(content=final_respose)
        
        else:
            results={"missing_employees_hris":"N/A"}
            final_respose=jsonable_encoder(results)
            return JSONResponse(content=final_respose)
            
    
    except mysql.connector.Error as ex:
        print("\nError At missing_employees_hris", ex)


@app.post('/KronosApi/manager_pay_period_status/')
async def manager_pay_period_status(request:Request):
    db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
    cursor=db.cursor()
    requested_date_select= await request.json()
              
    print("\nIn EMP_DATA")

    date_select = requested_date_select['date']
    

    try:
        cursor.execute("SELECT pay_period_biweekly FROM kronos.previous_pay_period WHERE pay_period_end_date=%s",(date_select,))
        pay_period_date=cursor.fetchone()
        print("Pay Period Date is : ",pay_period_date)
        pay_period_date_new = pay_period_date[0]
        
        #percent_mgr_unapp_TC , ytd_perc_mgr_uapp_new '&' ytd_perc_mgr_uapp_new as cum_sum_mgr_not_approve_TC
        #pay_period_biweekly is taken as  "pay_period_date" but now changed it to "pay_period_end_date"
        
        cursor.execute("select \
        pay_period_end_date as pay_period_date,\
        ytd_perc_mgr_uapp_new AS  ytd_percent_mgr_not_approve_TC,\
        percent_mgr_unapp_TC as percent_mgr_not_approve_TC ,\
        Total_no_of_unapp_TC as total_no_of_unapproved_TC_per_pay_period,\
        mgr_approved_TC as mgr_approve_TC,   \
        total_managers_count_per_pay_Period as total_managers_count_per_pay_period, \
        unapproved_time_cards_count as mgr_not_approve_TC, ytd_mgrs_unappr,ytd_total_mgrs,unappr_TC_Pay_Period AS total_no_TC,ytd_total_TC,ytd_unappr_TC,\
        unappr_TC_Pay_Period_Perc,ytd_perc_unappr_TC\
        from kronos.previous_pay_period where pay_period_biweekly = %s order by pay_period_biweekly desc ;",(pay_period_date_new,))
        columns = cursor.description 
        result = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        final = result[0]
        final_respose=jsonable_encoder(final)
        return JSONResponse(content=final_respose)
        
    except:
        d= {
            "total_no_of_unapproved_TC_per_pay_period": 0,
            "cum_sum_mgr_not_approve_TC": 0,
            "mgr_approve_TC": 0,
            "mgr_not_approve_TC": 0,
            "percent_mgr_not_approve_TC": 0,
            "total_managers_count_per_pay_period": 0
        }
        final_respose=jsonable_encoder(d)
        return JSONResponse(content=final_respose)
        


@app.get('/KronosApi/saturday_week_list/')
async def saturday_week_list():
    try:
        host, user, pswd, db = db_config()
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        #db=mysql.connector.connect(user=user, password= pswd, host= host, database= db,auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        print("\nsaturday_week_list Api Started")
        
        today = date.today()
        cursor.execute("SELECT saturday_week FROM kronos.missed_swipes_by_week ORDER BY id DESC")
        #cursor.execute("SELECT saturday_week FROM kronos.missed_swipes_by_week ORDER BY  STR_TO_DATE(saturday_week','%m/%d/%Y') DESC ")
        columns = cursor.description
        results = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        result={"saturday_week_list":results}
        final_respose=jsonable_encoder(result)
        return JSONResponse(content=final_respose)
    
    except mysql.connector.Error as ex:
        print("\nError At Emp_Data", ex)

        
@app.get('/KronosApi/pay_period_list/')
async def pay_period_list():
    try:
        host, user, pswd, db = db_config()
        db=mysql.connector.connect(user='MySqlUser', password= 'Admin@1234', host= '10.0.1.227', database= 'kronos',auth_plugin = 'mysql_native_password')
        #db=mysql.connector.connect(user=user, password= pswd, host= host, database= db,auth_plugin = 'mysql_native_password')
        cursor=db.cursor()
        print("\nPay_Period_List Api Started")
        today = date.today()
        cursor.execute("SELECT pay_period_end_date AS pay_period_biweekly FROM kronos.previous_pay_period_dates ORDER BY id DESC ")
        columns = cursor.description
        results = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
        result={"pay_period_list":results}
        final_respose=jsonable_encoder(result)
        return JSONResponse(content=final_respose)
    
    except mysql.connector.Error as ex:
        print("\nError At Emp_Data", ex)
        
    

if __name__=="__main__":
    host = get_host()
    app.run(host='10.0.1.224',port=5001, debug = True)
