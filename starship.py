import pandas as pd
import numpy.core.multiarray 

# Convert to CSV
df = pd.read_parquet('validation-00000-of-00001.parquet')
df.to_csv('dataset.csv')


# Preview the first 20 rows of the CSV
df_preview = pd.read_csv('dataset.csv', nrows=20)
print(df_preview)
import pandas as pd



import ast  # To convert string representation of lists/dicts to actual lists/dicts

import pandas as pd
import json
import re

def convert_answers(answer):
    print("Before transformation:", answer)

    # Convert NumPy array representations to lists and remove dtype info
    answer_str = str(answer).replace('array(', '[').replace(')', ']').replace('dtype=object', '').strip()

    # Ensure all keys are quoted
    answer_str = re.sub(r'([a-zA-Z_]\w*)\s*:', r'"\1":', answer_str)

    # Correctly format the text field to ensure it's treated as a string
    answer_str = re.sub(r'"text":\s*\[([^]]*?)\]', lambda m: f'"text": [["{m.group(1).strip()}"]]', answer_str)

    # Replace single quotes with double quotes for valid JSON format
    answer_str = answer_str.replace("'", "\"")
    
    # Strip leading/trailing whitespace
    answer_str = answer_str.strip()

    # Remove any trailing commas in lists or objects
    answer_str = re.sub(r',\s*([\]}])', r'\1', answer_str)

    # Print the transformed string for debugging
    print("Transformed answer before JSON decoding:", answer_str)

    try:
        # Load the answer as JSON
        return json.loads(answer_str)
    except json.JSONDecodeError as e:
        print(f"JSON decoding error for answer: {answer_str}")
        raise e

def process_csv(input_file, output_file):
    # Read the CSV file
    df = pd.read_csv(input_file)

    # Apply the transformation to the 'answers' column
    df['answers'] = df['answers'].apply(lambda x: convert_answers(x) if isinstance(x, str) else x)

    # Save the modified DataFrame to a new CSV file
    df.to_csv(output_file, index=False)

if __name__ == "__main__":
    input_file = 'dataset.csv'  
    output_file = 'cleaned_dataset.csv'  # Name for the transformed CSV file
    process_csv(input_file, output_file)










