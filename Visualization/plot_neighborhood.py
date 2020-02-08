import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import csv

table = pd.read_csv("Statistics_Neighborhood.csv")
table.plot.bar(x = 'Neighborhood', y = 'Crimes_Count', color = 'purple')
plt.title("Crime Count By Neighborhood NY")
plt.xlabel("Neighborhood")
plt.ylabel("Count")
plt.savefig("Crime count by neighborhood.eps", format='eps', bbox_inches='tight', pad_inches=0, dpi=1200)
#plt.show()