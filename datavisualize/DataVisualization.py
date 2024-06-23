import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
plt.style.use('default')

price = [48000,54000,57000,49000,47000,45000]
year = [2015,2016,2017,2018,2019,2020]

#plt.plot(year,price)
#plt.show()
#batman = pd.read_csv('/Users/pankajpachori/PycharmProjects/datavisualize/sharma-kohli.csv')
#print(batman)
#plt.plot(batman['index'],batman['V Kohli'],color='green',linestyle='solid',linewidth=3,marker='+',markersize=10,label='Virat')
#plt.plot(batman['index'],batman['RG Sharma'],color='black',linestyle='dashed',marker='.',markersize=5,label='Rohit')
#plt.title('Rohit sharma Vs Virat Kohli IPL Carrier')
#plt.xlabel('Season')
#plt.ylabel('Score')
#plt.legend(loc='upper right')
#plt.grid()
#plt.show()

df = pd.read_csv('/Users/pankajpachori/PycharmProjects/datavisualize/batter.csv')
print(df.head(50))