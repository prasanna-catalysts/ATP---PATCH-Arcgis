from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import geopandas as gpd
import os
import io

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
        raise FileNotFoundError(f"❌ File '{name}' not found in Drive.")
    return files[0]["id"]

# --- Step 2: Download GPKG from Drive ---
gpkg_filename = "RCHAttributes_NEW.gpkg"
output_folder_id = "1B8GFyekLEny2MpG1-7O1JQ1z1GLmopMA"  # <-- adjust if needed
gpkg_file_id = get_file_id_by_name(gpkg_filename, folder_id=output_folder_id)

gpkg_local_path = "temp_gpkg_upload.gpkg"
request = drive_service.files().get_media(fileId=gpkg_file_id)
with open(gpkg_local_path, "wb") as f:
    downloader = MediaIoBaseDownload(f, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
print(f"✅ Downloaded '{gpkg_filename}' from Google Drive.")

# --- Step 3: Load GPKG using GeoPandas ---
gdf = gpd.read_file(gpkg_local_path)
print(f"✅ Loaded {len(gdf)} features from GPKG.")
print("📋 GPKG Columns:", gdf.columns.tolist())

# --- Step 4: ArcGIS Login ---
username = os.getenv("ARCGIS_USERNAME")
password = os.getenv("ARCGIS_PASSWORD")
gis = GIS("https://www.arcgis.com", username, password)

if not gis.users.me:
    raise Exception("❌ ArcGIS login failed. Check credentials.")
print(f"✅ Logged in to ArcGIS as: {gis.users.me.username}")

# --- Step 5: Access Feature Layer ---
feature_item_id = "04fb50c636b04a0da9390256f9be1b36"  # replace if different
item = gis.content.get(feature_item_id)
flc = FeatureLayerCollection.fromitem(item)
layer = flc.layers[0]

# --- Step 6: Query Features ---
features = layer.query(where="1=1", out_fields="*", return_geometry=False).features
print(f"✅ Retrieved {len(features)} features from ArcGIS layer.")

# --- Step 7: Prepare Attribute Updates ---
# --- Step 7: Prepare Attribute Updates ---
updates = []
oid_field = layer.properties.objectIdField  # Dynamically fetch correct ObjectID field name
print(f"ℹ️ Using Object ID field: {oid_field}")

for feature in features:
    attr = feature.attributes
    subdistric = attr.get("subdistric")
    
    match = gdf[gdf["subdistric"] == subdistric]
    if not match.empty:
        row = match.iloc[0]
        updates.append({
            "attributes": {
                oid_field: attr[oid_field],  # Correct ID field dynamically
                "Attributes": row.get("Attributes", attr.get("Attributes")),
                "Total": row.get("Total", attr.get("Total")),
                "Perc": row.get("Perc", attr.get("Perc")),
                "Norm": row.get("Norm", attr.get("Norm"))
            }
        })
    else:
        print(f"⚠️ No match for subdistric: {subdistric}")


# --- Step 8: Apply Updates ---
if updates:
    result = layer.edit_features(updates=updates)
    print("✅ Attribute updates pushed to ArcGIS.")
else:
    print("⚠️ No updates to push.")

# --- Step 9: Cleanup ---
os.remove(gpkg_local_path)
print("🧹 Temporary GPKG file removed.")
