from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import StringIO
from fastapi.responses import JSONResponse


import os
app = FastAPI()

# Serve static files at /static
# app.mount("/static", StaticFiles(directory="C:/Users/ravin/Desktop/poc/Restapi_project/static"), name="static")
# app.mount("/static", StaticFiles(directory="static"), name="static")

# # Serve index.html at root
# @app.get("/")
# async def serve_index():
#     return FileResponse("C:/Users/ravin/Desktop/poc/Restapi_project/static/index.html")
# update code 

# Define the base directory dynamically using __file__
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Mount the static folder
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

# Serve index.html at root
@app.get("/")
def serve_index():
    file_path = os.path.join(BASE_DIR, "static", "index.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    else:
        return {"error": "index.html not found"}
    
# Endpoint to fetch images based on province
@app.get("/images/{province}")
def get_images(province: str):
    static_dir = os.path.join(BASE_DIR, "static")
    province_images = []

    # Iterate over files in the static directory
    for file in os.listdir(static_dir):
        # Check if the filename contains the province name (case-insensitive)
        if province.lower() in file.lower() and file.endswith(('.png', '.jpg', '.jpeg')):
            province_images.append(f"/static/{file}")

    if not province_images:
        return JSONResponse({"error": f"No images found for province: {province}"}, status_code=404)
    
    return province_images

# Add CORS Middleware to handle cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow requests from any origin
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all HTTP headers
)

# Azure Blob Storage Configuration
STORAGE_ACCOUNT_NAME = "storageaccountdemonpoc"
STORAGE_ACCOUNT_KEY = "f4NBocOPh7w+ugJp52D6wx29fHUmvrUOxIQ40jDNDspSJAhznCmwlDxh5fz5z5ACGUryq+6FSRFO+AStOV/4dA=="

# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
    credential=STORAGE_ACCOUNT_KEY
)

# Columns to keep
COLUMNS_TO_KEEP = [
    "latitude", "longitude", "name", "population", "price_per_m2",
    "province", "rooms", "year", "GST", "average_price",
    "total_tax_rate", "facility", "population_price_interaction", "rooms_latitude_interaction"
]

# Function to process flattened housing data
@app.get("/data/flattened_housing")
def get_flattened_housing_data(page: int = Query(1, ge=1), limit: int = Query(100, ge=1, le=1000)):
    try:
        container_client = blob_service_client.get_container_client("gold1")
        combined_data = pd.DataFrame()
        blob_list = container_client.list_blobs()

        for blob in blob_list:
            if "flattened_housing_data.csv/part-" in blob.name and blob.name.endswith(".csv"):
                blob_client = container_client.get_blob_client(blob.name)
                blob_data = blob_client.download_blob().readall()

                # Convert blob data to DataFrame
                csv_data = StringIO(blob_data.decode('utf-8'))
                temp_df = pd.read_csv(csv_data)

                # Keep only specified columns
                temp_df = temp_df[temp_df.columns.intersection(COLUMNS_TO_KEEP)]

                # Combine data
                combined_data = pd.concat([combined_data, temp_df], ignore_index=True)

        # Paginate the DataFrame
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_data = combined_data.iloc[start_idx:end_idx]

        return paginated_data.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Function to process advanced predictions data
@app.get("/data/advanced_predictions")
def get_advanced_predictions(page: int = Query(1, ge=1), limit: int = Query(100, ge=1, le=1000)):
    try:
        container_client = blob_service_client.get_container_client("machinelearning")
        blob_name = "advanced_predictions.csv/part-00001-tid-1766800978290826300-47427e24-eefb-4137-8c74-433d6a353413-8153-1-c000.csv"
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall()

        # Convert blob data to DataFrame
        csv_data = StringIO(blob_data.decode('utf-8'))
        temp_df = pd.read_csv(csv_data)

        # Keep only specified columns
        temp_df = temp_df[temp_df.columns.intersection(COLUMNS_TO_KEEP)]

        # Paginate the DataFrame
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_data = temp_df.iloc[start_idx:end_idx]

        return paginated_data.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Function to process affordability by region data
@app.get("/data/affordability_by_region")
def get_affordability_by_region(page: int = Query(1, ge=1), limit: int = Query(100, ge=1, le=1000)):
    try:
        container_client = blob_service_client.get_container_client("machinelearning")
        blob_name = "affordability_by_region.csv/part-00000-tid-6073685301762930151-f38712bc-8202-40be-be2e-aaff32ff281f-8160-1-c000.csv"
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall()

        # Convert blob data to DataFrame
        csv_data = StringIO(blob_data.decode('utf-8'))
        temp_df = pd.read_csv(csv_data)

        # Ensure all required columns exist
        required_columns = ["province", "avg_price_per_m2", "avg_total_tax_rate", "affordability_index"]
        missing_columns = [col for col in required_columns if col not in temp_df.columns]
        if missing_columns:
            return {"error": f"Missing columns in CSV: {missing_columns}"}

        # Keep only the necessary columns
        temp_df = temp_df[required_columns]

        # Paginate the DataFrame
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_data = temp_df.iloc[start_idx:end_idx]

        return paginated_data.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}

# Function to process regional housing characteristics data
@app.get("/data/regional_housing_characteristics")
def get_regional_housing_characteristics(page: int = Query(1, ge=1), limit: int = Query(100, ge=1, le=1000)):
    try:
        container_client = blob_service_client.get_container_client("machinelearning")
        blob_name = "regional_housing_characteristics.csv/part-00000-tid-7691442025627459831-23e8a5a9-ca39-4456-9654-4e80e67d2bca-8170-1-c000.csv"
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall()

        # Convert blob data to DataFrame
        csv_data = StringIO(blob_data.decode('utf-8'))
        temp_df = pd.read_csv(csv_data)

        # Paginate the DataFrame
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        paginated_data = temp_df.iloc[start_idx:end_idx]

        return paginated_data.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}
