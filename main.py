import pandas as pd
import requests
from bs4 import BeautifulSoup
import mysql.connector


url='https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)'

html=requests.get(url).content
soup=BeautifulSoup(html,'html.parser')
table=soup.find_all('table')[0]
df=pd.read_html(str(table))[0]

df=df.dropna(subset=['UN continental region[4]','UN statistical subregion[4]'])
df=df.drop("Change",axis='columns')

df.to_csv(r"C:\Users\Dell\Downloads\countries.csv",index=False)

mydb=mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="root",
    database="python_project"
)

mycursor=mydb.cursor()

mycursor.execute("drop table if exists countries")
mycursor.execute("create table countries(id int auto_increment primary key,country varchar(100),continent varchar(40),subregion varchar(40),population2022 bigint,population2023 long)")

for i,row in df.iterrows():
    country=row[0]
    continent=row[1]
    subregion=row[2]
    population2022=row[3]
    population2023=row[4]
    sql="insert into countries(country,continent,subregion,population2022,population2023) values (%s,%s,%s,%s,%s)"
    val=(country,continent,subregion,population2022,population2023)
    mycursor.execute(sql,val)

mydb.commit()


mycursor.execute("select * from countries")
result=mycursor.fetchall()

df2=pd.DataFrame(result)
print(df2.head(15))
