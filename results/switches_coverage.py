import pandas as pd
reference_switches_number = 10379
simulation_data = pd.read_csv('sumo_data/switches.csv', delimiter=';').shape[0]

print("Switches Coverage:")
print("==========================")
print('Reference data:')
print(f"{reference_switches_number} switches")
print('Simulation data:')
print(f"{simulation_data} switches")
print('Coverage:')
print(f'{simulation_data / reference_switches_number * 100:.2f}%')