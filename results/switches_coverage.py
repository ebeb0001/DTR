import pandas as pd
reference_switches_number = 10379

tracks = pd.read_csv('sumo_data/main_tracks.csv', delimiter=';')
switches = dict()
for _, row in tracks.iterrows():
    start = row['Departure_switch']
    end = row['Arrival_switch']
    if start != end:
        if start not in switches:
            switches[start] = 0
        switches[start] += 1
        if end not in switches:
            switches[end] = 0
        switches[end] += 1
real_switches = [s for s in switches if switches[s] > 2]

print("Switches Coverage:")
print("==========================")
print('Reference data:')
print(f"{reference_switches_number} switches")
print('Simulation data:')
print(f"{len(real_switches)} switches")
print('Coverage:')
print(f'{len(real_switches) / reference_switches_number * 100:.2f}%')