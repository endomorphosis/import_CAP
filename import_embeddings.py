import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.http import models
import os
import sys

folder_name = '/storage/teraflopai/Caselaw_Access_Project_embeddings'
files =     ['parquet_file_1.parquet',
            'parquet_file_3.parquet',
            'parquet_file_0.parquet',
            'parquet_file_2.parquet',
            'parquet_file_4.parquet']
  
# Load the Parquet file
df = pd.read_parquet(os.path.join(folder_name, files[0]))

# Initialize Qdrant client
client = QdrantClient(url="http://localhost:6333")  # Replace with your Qdrant server URL

# Define the collection name
collection_name = 'your_collection_name'

# Define the collection schema (adjust this to match your data's structure)
client.recreate_collection(
    collection_name=collection_name,
    vectors_config=models.VectorParams(size=df['embeddings'].iloc[0].shape[0], distance=models.Distance.COSINE),
)

# Prepare the points to be inserted
points = [
    models.PointStruct(
        id=index,
        vector=embedding.tolist(),  # Convert embedding to list
        payload={"text": text}
    )
    for index, (text, embedding) in df.iterrows()
]

# Insert points into Qdrant
client.upsert(
    collection_name=collection_name,
    points=points
)

print("Data successfully ingested into Qdrant")