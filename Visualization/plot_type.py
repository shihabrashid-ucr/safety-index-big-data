import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import csv

#fig = plt.figure(figsize=(30,15))
table = pd.read_csv("Statistics_Crime_Type.csv")

table.plot.bar(x = 'Crime_Type', y = 'Crimes_Count', color = 'red')
plt.title("Crime Count By Type NY")
plt.xlabel("Crime Type")
plt.ylabel("Count")
plt.savefig("crime count by type.eps", format='eps', bbox_inches='tight', pad_inches=0, dpi=1200)
#plt.show()