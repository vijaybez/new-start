import pandas as pd

#Collect population information in the US for all zipcodes
pop =pd.read_csv('population.csv')
pop_new=pop.drop(labels=['aggregate'],axis=1)
pop_new.columns=['zipcode','2016','2015','2014','2013','2012','2011','2010']
pop_pivot = pd.melt(pop_new,id_vars=['zipcode'],value_vars=['2016','2015','2014','2013','2012','2011','2010'])
pop_pivot.columns=['zipcode','year','population']

#collect zipcode latitude and longitude info
zip_lat = pd.read_csv('us_zipcode.csv',sep=';')
zip_lat=zip_lat.drop(labels=['Timezone','Daylight savings time flag','geopoint'],axis=1)
merge_pop=pd.merge(pop_pivot,zip_lat, left_on='zipcode',right_on='Zip' )

# collect full state names 
statenames = pd.read_csv('statenames.csv')
statenames.columns=['state_full_name','state_abbrev','state_code']
merge_pop_new=pd.merge(merge_pop,statenames, left_on='State',right_on='state_code' )
merge_pop_new=merge_pop_new.drop(['state_code'],axis=1)
merge_pop_new.to_csv('merge_pop.csv')
