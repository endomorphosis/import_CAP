import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.http import models
import os
import sys



def insert_from_parquet(folder_name, file):
    # Load the Parquet file
    df = pd.read_parquet(os.path.join(folder_name, file))

    # Initialize Qdrant client
    client = QdrantClient(host="localhost", port=6333)# Replace with your Qdrant server URL

    # Define the collection name
    collection_name = 'Caselaw_Access_Project'

    # Define the collection schema (adjust this to match your data's structure)
    client.recreate_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(size=df['embeddings'].iloc[0].shape[0], distance=models.Distance.COSINE),
    )

    # Chunk size for generating points
    chunk_size = 1

    # Prepare the points to be inserted in chunks
    for start in range(0, len(df), chunk_size):
        end = min(start + chunk_size, len(df))
        chunk_df = df.iloc[start:end]

        points = []
        for index, (text, embedding) in chunk_df.iterrows():
            points.append(models.PointStruct(
                id=index,
                vector=embedding.tolist() if embedding is not None else None,  # Convert embedding to list if not None
                payload={"text": text}
            ))

        client.upsert(
            collection_name=collection_name,
            points=points
        )
        
        print("Data successfully ingested into Qdrant")

    print("All data successfully ingested into Qdrant from Parquet file {}".format(file))
    return True
# Define the folder containing the Parquet files

folder_name = '/storage/teraflopai/Caselaw_Access_Project_embeddings'
files = ['parquet_file_0.parquet',
         'parquet_file_1.parquet',
         'parquet_file_2.parquet',
         'parquet_file_3.parquet',
         'parquet_file_4.parquet']

# Insert data from each Parquet file
for file in files:
    insert_from_parquet(folder_name, file)
    print("Data successfully ingested into Qdrant from Parquet file {}".format(file))
    print("All data successfully ingested into Qdrant from Parquet files")