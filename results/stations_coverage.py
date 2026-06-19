import pandas as pd
macro_reference_data = (
    pd.read_csv('data/operational_points.csv', delimiter=';').filter(items=['PTCAR ID', 'Classification EN'])
)
condition = macro_reference_data['Classification EN'] == 'Station'
macro_reference_data = macro_reference_data.where(condition)
macro_simulation_data = (
    pd.read_csv('sumo_data/stations.csv', delimiter=';').filter(items=['ID', 'Classification'])
)
condition = macro_simulation_data['Classification'] == 'Station'
macro_simulation_data = macro_simulation_data.where(condition)
macro_total_stations = macro_reference_data.shape[0]

print("Stations Coverage (for Macro simulation):")
print("==========================")
print('Reference data:')
print(f"{macro_total_stations} stations")
print('Simulation data:')
print(f"{macro_simulation_data.shape[0]} stations")

macro_reference_data = set(macro_reference_data['PTCAR ID'].dropna())
macro_simulation_data = set(macro_simulation_data['ID'].dropna())

missing_stations = 0
for station in macro_reference_data:
    if station not in macro_simulation_data:
        missing_stations += 1
print('Detected stations:')
print(f'{(macro_total_stations - missing_stations) / macro_total_stations * 100:.2f}%')

micro_reference_data = pd.read_csv('data/station_platforms.csv', delimiter=';').filter(items=['PTCAR ID'])
micro_reference_data.drop_duplicates(inplace=True)
micro_simulation_data = pd.read_csv('station_track_assigned.csv', delimiter=',').filter(items=['Station_ID'])
micro_simulation_data.drop_duplicates(inplace=True)
micro_total_stations = micro_reference_data.shape[0]

missing_stations = 0
for station in micro_reference_data['PTCAR ID']:
    if station not in micro_simulation_data['Station_ID'].values:
        missing_stations += 1

print("\nStations Coverage (for Micro simulation):")
print("==========================")
print('Reference data:')
print(f"{micro_total_stations} stations")
print('Simulation data:')
print(f"{micro_simulation_data.shape[0]} stations")
print('Detected stations:')
print(f'{(micro_total_stations - missing_stations) / micro_total_stations * 100:.2f}%')