import pandas as pd
import matplotlib.pyplot as plt

# Create a dictionary with the data for the DataFrame
data = {'TripsPerDay.py': [26, 23, 22],
        'EmpresasFind.py': [62, 58, 57],
        'viajes_concurridos.py': [77, 76, 73]}

df = pd.DataFrame(data, index=['2-Nod', '3-Nod', '5-Nod'])

df.plot(kind='bar', title='My Bar Plot')

plt.savefig('compare_nodes.png')
