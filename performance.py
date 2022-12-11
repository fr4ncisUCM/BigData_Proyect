import pandas as pd
import matplotlib.pyplot as plt

# Create a dictionary with the data for the DataFrame
data = {'TripsPerDay.py': [26, 23, 22],
        'EmpresasFind.py': [62, 58, 57],
        'Viajes_concurridos.py': [77, 76, 73]}

# Create the DataFrame using the dictionary and specify the index and column names
df = pd.DataFrame(data, index=['2-Nod', '3-Nod', '5-Nod'])

# Use the DataFrame.plot() method to create a bar plot
df.plot(kind='bar', title='My Bar Plot')

# Save the plot to a file
plt.savefig('my_plot.png')
