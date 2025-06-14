import os
import io
import pandas as pd
import geopandas as gpd
import fiona
import gc
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- Google Drive Setup ---
SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ['https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

# --- File/folder setup ---
SHARED_FOLDER_NAME = "Final Anantpur Data API"
INPUT_SUBFOLDER = "Input data"
OUTPUT_SUBFOLDER = "output data push to arcgis"
ARC_FOLDER = "Arcgis Data"

CSV_FILE_NAME = "mandal_weights.csv"
GPKG_INPUT_NAME = "RCHAttributes_input.gpkg"
GPKG_OUTPUT_NAME = "RCHAttributes_NEW.gpkg"

# --- Utility Functions ---
def find_shared_folder_id_by_name(folder_name):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
    results = drive_service.files().list(
        q=query,
        spaces='drive',
        fields="files(id, name)",
        corpora="allDrives",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True
    ).execute()
    for file in results.get('files', []):
        if file['name'] == folder_name:
            return file['id']
    raise FileNotFoundError(f"Shared folder '{folder_name}' not found.")

def get_subfolder_id(parent_id, subfolder_name):
    query = f"'{parent_id}' in parents and name = '{subfolder_name}' and mimeType = 'application/vnd.google-apps.folder'"
    results = drive_service.files().list(
        q=query,
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    files = results.get("files", [])
    if not files:
        raise FileNotFoundError(f"Subfolder '{subfolder_name}' not found inside parent ID {parent_id}")
    return files[0]["id"]

def get_file_id(folder_id, file_name):
    query = f"'{folder_id}' in parents and name = '{file_name}'"
    results = drive_service.files().list(
        q=query,
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    files = results.get("files", [])
    if not files:
        raise FileNotFoundError(f"File '{file_name}' not found in folder ID {folder_id}")
    return files[0]["id"]

# --- Get all required IDs ---
base_folder_id = find_shared_folder_id_by_name(SHARED_FOLDER_NAME)
arcgis_folder_id = get_subfolder_id(base_folder_id, ARC_FOLDER)
input_folder_id = get_subfolder_id(arcgis_folder_id, INPUT_SUBFOLDER)
output_folder_id = get_subfolder_id(arcgis_folder_id, OUTPUT_SUBFOLDER)

# --- Download CSV ---
csv_file_id = get_file_id(base_folder_id, CSV_FILE_NAME)
csv_data = drive_service.files().get_media(fileId=csv_file_id).execute()
with open("temp_weights.csv", "wb") as f:
    f.write(csv_data)

# --- Download GPKG ---
gpkg_file_id = get_file_id(input_folder_id, GPKG_INPUT_NAME)
gpkg_data = drive_service.files().get_media(fileId=gpkg_file_id).execute()
with open("temp_input.gpkg", "wb") as f:
    f.write(gpkg_data)

# --- Load data ---
gdf = gpd.read_file("temp_input.gpkg", layer=fiona.listlayers("temp_input.gpkg")[0])
weights = pd.read_csv("temp_weights.csv")

# --- Normalize names for join ---
gdf["sublower"] = gdf["subdistric"].astype(str).str.lower().str.strip()
weights["sublower"] = weights["mandal_name"].astype(str).str.lower().str.strip()

# --- Define attribute mappings ---
perc_fields = {
    "Anemia Improvement": "anemia_improvement_percentage",
    "Anemia Prevalence": "anemia_prevalence_percentage",
    "No Visit": "no_visit_percentage",
    "Premature Birth": "premature_birth_percentage",
    "Still Birth": "stillbirth_percentage",
    "Teenage Pregnancy": "teenage_pregnancy_percentage",
    "Timely Visit": "timely_visit_percentage"
}
total_fields = {
    "Anemia": "total_anemia",
    "No Visit": "no_visit_total",
    "Premature Birth": "premature_birth_total",
    "Still Birth": "count_stillbirth"
}

# --- Update logic ---
def update_values(row):
    att = row["Attributes"]
    sub = row["sublower"]
    subset = weights[weights["sublower"] == sub]
    if subset.empty:
        return pd.Series([row["Total"], row["Perc"], row["Norm"]])
    w = subset.iloc[0]
    total = w.get(total_fields.get(att), row["Total"])
    perc = w.get(perc_fields.get(att), row["Perc"])
    norm = w.get(f"{perc_fields.get(att)}_normalized", row["Norm"]) if perc_fields.get(att) else row["Norm"]
    return pd.Series([total, perc, norm])

gdf[["Total", "Perc", "Norm"]] = gdf.apply(update_values, axis=1)

# --- Round Total to integer, Perc/Norm to 2 decimal places ---
gdf["Total"] = gdf["Total"].round(0).astype("Int64")
gdf["Perc"] = gdf["Perc"].round(2)
gdf["Norm"] = gdf["Norm"].round(2)

# --- Clean up column ---
gdf.drop(columns=["sublower"], inplace=True)

# --- Save updated GPKG locally ---
updated_path = "temp_output.gpkg"
gdf.to_file(updated_path, layer=fiona.listlayers("temp_input.gpkg")[0], driver="GPKG")

# --- Upload to Drive ---
media = MediaFileUpload(updated_path, mimetype="application/geopackage+sqlite3", resumable=True)
existing_files = drive_service.files().list(
    q=f"'{output_folder_id}' in parents and name='{GPKG_OUTPUT_NAME}'",
    supportsAllDrives=True,
    includeItemsFromAllDrives=True
).execute()

if existing_files["files"]:
    file_id = existing_files["files"][0]["id"]
    drive_service.files().update(fileId=file_id, media_body=media).execute()
    print("✅ Existing GPKG file updated on Drive.")
else:
    drive_service.files().create(
        body={'name': GPKG_OUTPUT_NAME, 'parents': [output_folder_id]},
        media_body=media,
        supportsAllDrives=True
    ).execute()
    print("✅ New GPKG uploaded to Drive.")

# --- Clean up all temp files ---
del gdf
gc.collect()
for f in ["temp_input.gpkg", "temp_output.gpkg", "temp_weights.csv"]:
    try:
        os.remove(f)
    except Exception as e:
        print(f"⚠️ Couldn't delete {f}: {e}")
