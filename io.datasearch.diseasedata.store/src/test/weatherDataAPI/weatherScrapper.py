
import requests
import lxml.html as lh
import pandas as pd
import json 


country_code = 2120
start_day = 30
start_month = 7
start_year = 2020
end_day = 30
end_month = 7
end_year = 2020

url = ("http://www.meteomanz.com/sy2?l=1&cou="+str(country_code) + "&ind=00000&d1=" +str(start_day) + "&m1=" + str(start_month).zfill(2) + "&y1="+ str(start_year)+
		'&d2='+str(end_day) + '&m2=' + str(end_month).zfill(2) +'&y2='+ str(end_year))

#http://www.meteomanz.com/sy2?l=1&cou=2120&ind=00000&d1=30&m1=07&y1=2020&d2=30&m2=07&y2=2020
#http://www.meteomanz.com/sy2?l=1&cou=2120&ind=00000&d1=30&m1=7&y1=2020&d2=30&m2=7&y2=2020


print(url)


def scrapeData(day, month, year):
	country_code = 2120
	start_day = day
	start_month = month
	start_year = year
	end_day = day
	end_month = month
	end_year = year

	url = ("http://www.meteomanz.com/sy2?l=1&cou="+str(country_code) + "&ind=00000&d1=" +str(start_day) + "&m1=" + str(start_month).zfill(2) + "&y1="+ str(start_year)+
			'&d2='+str(end_day) + '&m2=' + str(end_month).zfill(2) +'&y2='+ str(end_year))


	page = requests.get(url)
	doc = lh.fromstring(page.content)

	tr_elements = doc.xpath('//tr')

	col = []
	i=0

	for t in tr_elements[0]:
		i+=1
		name=t.text_content()
		col.append((name,[]))

	for j in range(1,len(tr_elements)):
		T=tr_elements[j]

		i=0


		for t in T.iterchildren():
			data=t.text_content()

			if i>0:
				try:
					data=int(data)
				except:
					pass
			col[i][1].append(data)
			i+=1

	Dict = {title:column for (title, column) in col}
	df = pd.DataFrame(Dict)
	return df


# 	page = requests.get(url)
# 	#Store the contents of the website under doc
# 	doc = lh.fromstring(page.content)
#     #Parse data that are stored between <tr>..</tr> of HTML
#     tr_elements = doc.xpath('//tr')
#     #Create empty list
#     col=[]

#     for t in tr_elements[0]:
#         i+=1
#         name=t.text_content()
# #         print('%d:"%s"'%(i,name))
#         col.append((name,[]))
#     #Since out first row is the header, data is stored on the second row onwards
#     for j in range(1,len(tr_elements)):
#         #T is our j'th row
#         T=tr_elements[j]

#         #If row is not of size 10, the //tr data is not from our table 
#         if len(T)!=10:
#             break

#         #i is the index of our column
#         i=0

#         #Iterate through each element of the row
#         for t in T.iterchildren():
#             data=t.text_content() 
#             #Check if row is empty
#             if i>0:
#             #Convert any numerical value to integers
#                 try:
#                     data=int(data)
#                 except:
#                     pass
#             #Append the data to the empty list of the i'th column
#             col[i][1].append(data)
#             #Increment i for the next column
#             i+=1
#     Dict={title:column for (title,column) in col}
#     df=pd.DataFrame(Dict)
#     return df

def getPercipitation(day, month, year):
	df = scrapeData(day, month, year)
	df2 = df
	df2["percipitation"] = df2["Prec.(mm)"]

	percip = df2[["Station", "percipitation"]]

	jsonArray = []

	for i in percip.values:
		data =  { "Station": i[0], 
				  "Percipitation":i[1] 
				}
		y = json.dumps(data)
		print(y)
		jsonArray.append(y)

	return jsonArray