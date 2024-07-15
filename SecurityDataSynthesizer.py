import pandas as pd

# Step 1: Split Data into A and B
def split_data(df):
    duplicate_threshold = 4
    duplicated_rows = df[df.duplicated(keep=False)]
    A_part1 = duplicated_rows.groupby(list(df.columns)).filter(lambda x: len(x) >= duplicate_threshold)
    A_part1_unique = A_part1.drop_duplicates()
    A_part2 = duplicated_rows
    A = pd.concat([A_part1_unique, A_part2], ignore_index=True)
    A['size'] = A.groupby(list(df.columns)).transform('size')
    A = A.drop_duplicates().reset_index(drop=True)
    B = df.drop_duplicates().merge(A.drop_duplicates(), on=list(df.columns), how='left', indicator=True)
    B = B[B['_merge'] == 'left_only'].drop(columns=['_merge'])
    return A, B

# Step 2: Create set C from B
def create_C(B):
    C = B.drop(columns=['src_port', 'size'])
    C['multiplicity'] = C.groupby(list(C.columns)).transform('size')
    C1 = C[C['multiplicity'] > 1].drop_duplicates().reset_index(drop=True)
    C2 = C[C['multiplicity'] == 1].drop_duplicates().reset_index(drop=True)
    return C1, C2

# Step 3: Reconstruct the list of values for src_port
def reconstruct_src_port(df, C1, C2):
    map_C1 = df[df.index.isin(C1.index)][['src_port']].reset_index(drop=True)
    map_C2 = df[df.index.isin(C2.index)][['src_port']].reset_index(drop=True)
    return map_C1, map_C2

# Step 4: Generate synthetic observations for C2
def generate_synthetic_C2(C2, map_C2):
    synthetic_C2 = C2.copy()
    synthetic_C2['src_port'] = map_C2['src_port']
    return synthetic_C2

# Main function to execute the entire pipeline
def main():
    try:
        url = 'https://raw.githubusercontent.com/VincentGranville/Main/main/iot_security.csv'
        df = pd.read_csv(url)
        print("Columns in the dataset:", df.columns)
        A, B = split_data(df)
        C1, C2 = create_C(B)
        map_C1, map_C2 = reconstruct_src_port(df, C1, C2)
        synthetic_C2 = generate_synthetic_C2(C2, map_C2)
        print(f"Length of A: {len(A)}")
        print(f"Length of B: {len(B)}")
        print(f"Length of C1: {len(C1)}")
        print(f"Length of C2: {len(C2)}")
        print(f"Length of synthetic C2: {len(synthetic_C2)}")
    except Exception as e:
        print(f"Error occurred: {str(e)}")

# Execute the main function
if __name__ == "__main__":
    main()
