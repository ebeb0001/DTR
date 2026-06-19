import pandas as pd
macro_reference_data = (
    pd.read_csv('data/operational_points.csv', delimiter=';').filter(items=['Classification EN', 'PTCAR ID'])
)
condition = macro_reference_data['Classification EN'] == 'Station'
macro_reference_data = macro_reference_data.where(condition)
macro_simulation_data = (
    pd.read_csv('sumo_data/stations.csv', delimiter=';').filter(items=['Classification', 'ID'])
)
condition = macro_simulation_data['Classification'] == 'Station'
macro_simulation_data = macro_simulation_data.where(condition)

print("Stations Coverage (for Macro simulation):")
print("==========================")
print('Reference data:')
print(f"{macro_reference_data.shape[0]} stations")
print('Simulation data:')
print(f"{macro_simulation_data.shape[0]} stations")
print('Coverage:')
print(f'{macro_simulation_data.shape[0] / macro_reference_data.shape[0] * 100:.2f}%')

micro_reference_data = pd.read_csv('data/station_platforms.csv', delimiter=';').filter(items=['PTCAR ID'])
micro_reference_data.drop_duplicates(inplace=True)
micro_simulation_data = pd.read_csv('station_track_assigned.csv', delimiter=',').filter(items=['Station_ID'])
micro_simulation_data.drop_duplicates(inplace=True)

print("\nStations Coverage (for Micro simulation):")
print("==========================")
print('Reference data:')
print(f"{micro_reference_data.shape[0]} stations")
print('Simulation data:')
print(f"{micro_simulation_data.shape[0]} stations")
print('Coverage:')
print(f'{micro_simulation_data.shape[0] / micro_reference_data.shape[0] * 100:.2f}%')