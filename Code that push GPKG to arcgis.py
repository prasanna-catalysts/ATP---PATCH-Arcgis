from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import geopandas as gpd
import io
import os

# --- Step 1: Google Drive Setup ---
SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ['https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

def get_file_id_by_name(name, folder_id=None):
    query = f"name='{name}'"
    if folder_id:
        query += f" and '{folder_id}' in parents"
    results = drive_service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])
    if not files:
        raise FileNotFoundError(f"File '{name}' not found in Drive.")
    return files[0]["id"]

# --- Step 2: Download GPKG ---
gpkg_filename = "RCHAttributes_NEW.gpkg"
output_folder_id = "1B8GFyekLEny2MpG1-7O1JQ1z1GLmopMA"
gpkg_drive_file_id = get_file_id_by_name(gpkg_filename, folder_id=output_folder_id)

gpkg_temp_path = "temp_gpkg_upload.gpkg"
gpkg_request = drive_service.files().get_media(fileId=gpkg_drive_file_id)
with open(gpkg_temp_path, "wb") as f:
    downloader = MediaIoBaseDownload(f, gpkg_request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

print(f"✅ Downloaded {gpkg_filename} from Google Drive.")

# --- Step 3: Read GPKG ---
gdf = gpd.read_file(gpkg_temp_path)
print(f"✅ Loaded {len(gdf)} features from GPKG.")
print("📋 GPKG Columns:", gdf.columns.tolist())

# --- Step 4: ArcGIS Setup ---
gis = GIS("https://www.arcgis.com", "ARCGIS_USERNAME", "ARCGIS_PASSWORD")
feature_item_id = "04fb50c636b04a0da9390256f9be1b36"
item = gis.content.get(feature_item_id)
flc = FeatureLayerCollection.fromitem(item)
layer = flc.layers[0]

# --- Step 5: Query all features from ArcGIS Layer ---
features = layer.query(where="1=1", out_fields="*", return_geometry=False).features
print(f"✅ Retrieved {len(features)} features from ArcGIS layer.")

# --- Step 6: Attribute Update by subdistric ---
updates = []
for feature in features:
    attr = feature.attributes
    subdistric = attr.get('subdistric')

    matched = gdf[gdf['subdistric'] == subdistric]
    if not matched.empty:
        row = matched.iloc[0]
        updates.append({
            "attributes": {
                #"GlobalID": attr["GlobalID"],  # Mandatory for ArcGIS updates
                "Attributes": row.get('Attributes', attr.get('Attributes')),
                "Total": row.get('Total', attr.get('Total')),
                "Perc": row.get('Perc', attr.get('Perc')),
                "Norm": row.get('Norm', attr.get('Norm'))
            }
        })
    else:
        print(f"⚠️ No match found for subdistric: {subdistric}")

# --- Step 7: Push Updates ---
if updates:
    result = layer.edit_features(updates=updates)
    print("✅ Attribute updates applied successfully.")
else:
    print("⚠️ No matching features found to update.")

# --- Step 8: Clean up ---
os.remove(gpkg_temp_path)
print("🧹 Temporary file cleaned up.")
