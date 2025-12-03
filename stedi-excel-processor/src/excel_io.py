def read_excel(file_path):
    import pandas as pd
    return pd.read_excel(file_path)

def write_excel(file_path, data_frame):
    import pandas as pd
    data_frame.to_excel(file_path, index=False)

def load_data_from_excel(files):
    data_frames = {}
    for file in files:
        data_frames[file] = read_excel(file)
    return data_frames

def save_responses_to_excel(output_file, responses):
    import pandas as pd
    response_df = pd.DataFrame(responses)
    write_excel(output_file, response_df)