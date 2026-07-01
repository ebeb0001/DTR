import pandas as pd
macro_reference_data : pd.DataFrame = (
    pd.read_csv('data/station_to_station.csv', delimiter=';').filter(items=['Gare de départ (id)']),
    pd.read_csv('data/station_to_station.csv', delimiter=';').filter(items=['Gare d\'arrivée (id)'])
)
macro_reference_data[0].rename(columns={'Gare de départ (id)': 'ID'}, inplace=True)
macro_reference_data[1].rename(columns={'Gare d\'arrivée (id)': 'ID'}, inplace=True)
macro_reference_data = pd.concat(macro_reference_data, axis=0)
macro_reference_data.drop_duplicates(inplace=True)
macro_simulation_data : pd.DataFrame = (
    pd.read_csv('sumo_data/station_to_station.csv', delimiter=';').filter(items=['Departure_station_id']),
    pd.read_csv('sumo_data/station_to_station.csv', delimiter=';').filter(items=['Arrival_station_id']),
)
macro_simulation_data[0].rename(columns={'Departure_station_id': 'ID'}, inplace=True)
macro_simulation_data[1].rename(columns={'Arrival_station_id': 'ID'}, inplace=True)
macro_simulation_data = pd.concat(macro_simulation_data, axis=0)
macro_simulation_data.drop_duplicates(inplace=True)

print("Stations Coverage (for Macro simulation):")
print("==========================")
print('Reference data:')
print(f"{macro_reference_data.shape[0]} stations")
print('Simulation data:')
print(f"{macro_simulation_data.shape[0]} stations")

macro_reference_data = set(macro_reference_data['ID'].dropna())
macro_simulation_data = set(macro_simulation_data['ID'].dropna())

missing_stations = 0
for station in macro_reference_data:
    if station not in macro_simulation_data:
        missing_stations += 1
print('Detected stations:')
print(f'{(len(macro_reference_data) - missing_stations) / len(macro_reference_data) * 100:.2f}%')

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
print(f"{micro_reference_data.shape[0]} stations")
print('Simulation data:')
print(f"{micro_simulation_data.shape[0]} stations")
print('Detected stations:')
print(f'{(micro_reference_data.shape[0] - missing_stations) / micro_reference_data.shape[0] * 100:.2f}%')