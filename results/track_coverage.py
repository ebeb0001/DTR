import pandas as pd
macro_reference_data = (
    pd.read_csv('data/station_to_station.csv', delimiter=';')
)
macro_simulation_data = (
    pd.read_csv('sumo_data/station_to_station.csv', delimiter=';')
)

print("Track Coverage (for Macro simulation):")
print("==========================")
print('Reference data:')
print(f"{macro_reference_data.shape[0]} tracks")
print('Simulation data:')
print(f"{macro_simulation_data.shape[0]} tracks")
print('Coverage:')
print(f'{macro_simulation_data.shape[0] / macro_reference_data.shape[0] * 100:.2f}%')

micro_reference_data = pd.read_csv('data/main_tracks.csv', delimiter=';')
micro_simulation_data = pd.read_csv('sumo_data/main_tracks.csv', delimiter=';')

print("\nTracks Coverage (for Micro simulation):")
print("==========================")
print('Reference data:')
print(f"{micro_reference_data.shape[0]} tracks")
print('Simulation data:')
print(f"{micro_simulation_data.shape[0]} tracks")
print('Coverage:')
print(f'{micro_simulation_data.shape[0] / micro_reference_data.shape[0] * 100:.2f}%')