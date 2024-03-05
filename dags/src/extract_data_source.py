import pandas as pd

class CSVDataReader:
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.df_track = None
        self.df_Trajectory = None

    def read_data(self):
        with open(self.csv_path, 'r') as file:
            lines = file.readlines()

        line_of_lists = [line.strip('\n').strip().strip(';').split(';') for line in lines]
        cols = line_of_lists.pop(0)
        track_c = cols[:4]
        trajectory_c = ['track_id'] + cols[4:]
        track_info = []
        trajectory_info = []

        for row in line_of_lists:
            track_id = row[0]

            # add the first 4 values to track_info
            track_info.append(row[:4])

            remaining_values = row[4:]
            # reshape the list into a matrix and add track_id
            trajectory_matrix = [['track_id'] + remaining_values[i:i + 6] for i in range(0, len(remaining_values), 6)]
            # add the matrix rows to trajectory_info
            trajectory_info = trajectory_info + trajectory_matrix

        self.df_track = pd.DataFrame(data=track_info, columns=track_c)
        self.df_Trajectory = pd.DataFrame(data=trajectory_info, columns=trajectory_c)


            
       