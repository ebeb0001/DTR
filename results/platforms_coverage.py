import pandas as pd
reference_data = pd.read_csv('data/station_platforms.csv', delimiter=';')
simulation_data = pd.read_csv('station_track_assigned.csv', delimiter=',')

print("Stations Coverage:")
print("==========================")
print('Reference data:')
print(f"{reference_data.shape[0]} platforms")
print('Simulation data:')
print(f"{simulation_data.shape[0]} platforms")
print('Coverage:')
print(f'{simulation_data.shape[0] / reference_data.shape[0] * 100:.2f}%')