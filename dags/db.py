import pymysql
import scratch

db_setting = {
    "host":"127.0.0.1",
    "port": 3306,
    "user":"root",
    "password":"qp8=FLgIcgGH|H)B",
    "db":"airflow",
    "charset":"utf8"
}
try:
    #建立 connection 物件
    conn = pymysql.connect(**db_setting)
    #建立 cursor 物件
    with conn.cursor() as cursor:
        #資料表的相關操作
        #新增 SQL 語法
        command = "INSERT INTO jobs (
            jobType, jobNo, jobName, jobNameSnippet, jobRole, jobRo, jobAddrNo, jobAddrNoDesc, jobAddress,
            description, descWithoutHighlight, optionEdu, period, periodDesc, applyCnt, applyType, applyDesc,
            custNo, custName, coIndustry, coIndustryDesc, salaryLow, salaryHigh, salaryDesc, s10, appearDate,
            appearDateDesc, optionZone, isApply, applyDate, isSave, descSnippet, tags, landmark, link, jobsource,
            jobNameRaw, custNameRaw, lon, lat, remoteWorkType, major, salaryType, dist, mrt, mrtDesc, JobCat, code,
            condition, jobCategory, company_employees, company_capital
        ) VALUES (
            %(jobType)s, %(jobNo)s, %(jobName)s, %(jobNameSnippet)s, %(jobRole)s, %(jobRo)s, %(jobAddrNo)s,
            %(jobAddrNoDesc)s, %(jobAddress)s, %(description)s, %(descWithoutHighlight)s, %(optionEdu)s,
            %(period)s, %(periodDesc)s, %(applyCnt)s, %(applyType)s, %(applyDesc)s, %(custNo)s, %(custName)s,
            %(coIndustry)s, %(coIndustryDesc)s, %(salaryLow)s, %(salaryHigh)s, %(salaryDesc)s, %(s10)s,
            %(appearDate)s, %(appearDateDesc)s, %(optionZone)s, %(isApply)s, %(applyDate)s, %(isSave)s,
            %(descSnippet)s, %(tags)s, %(landmark)s, %(link)s, %(jobsource)s, %(jobNameRaw)s, %(custNameRaw)s,
            %(lon)s, %(lat)s, %(remoteWorkType)s, %(major)s, %(salaryType)s, %(dist)s, %(mrt)s, %(mrtDesc)s,
            %(JobCat)s, %(code)s, %(condition)s, %(jobCategory)s, %(company_employees)s, %(company_capital)s
        )"