#!/usr/bin/env python3

from minio import Minio
from minio.error import S3Error
import os
import argparse
from tqdm import tqdm
import yaml
import urllib3

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Default items to download for each type
DEFAULT_ITEMS = {
    'kitti': [
        #'depth_left',
        #'full_cloud',
        #'height_map',
        'image_left',
        #'image_left_color',
        #'image_right',
        #'livox',
        #'local_dino_map',
        #'local_gridmap',
        #'rgb_map',
        #'stereo_colored_point_cloud',
        #'stereo_colored_point_cloud_gmf',
        #'velodyne_0',
        #'velodyne_1',
        'cmd',
        'controls',
        'current_position',
        'gps_odom',
        'multisense_imu',
        'novatel_imu',
        'shock_pos',
        'steering_angle',
        'super_odom',
        'tartanvo_odom',
        'traversability_breakdown',
        'traversability_cost',
        'wheel_rpm',
    ],
    'bags': [
        'info.yaml',
        'gps.npy',
        'config.yaml'
    ]
}

# Load file mapping from YAML
with open("./assets/files.yaml") as stream:
    FILE_MAP = yaml.safe_load(stream)

# Minio client configuration
access_key = "m7sTvsz28Oq3AicEDHFo"
secret_key = "YVPGh367RnrT7G33lG6DtbaeuFZCqTE6KabMQClw"
endpoint_url = "airlab-share-01.andrew.cmu.edu:9000"
bucket_name = "tartandrive2"

# Predefined directories to download
DOWNLOAD_DIRS = {
    'kitti': [
        '2023-10-26-14-42-35_turnpike_afternoon_fall', 
        #'2023-11-02-16-00-43_down_meadows', 
        #'2023-11-14-14-24-21_gupta', 
        #'2023-11-14-14-26-22_gupta', 
        #'2023-11-14-14-34-53_gupta', 
        #'2023-11-14-14-45-20_gupta', 
        #'2023-11-14-14-52-22_gupta', 
        #'2023-11-14-15-02-21_figure_8', 
        #'2023-11-14-15-11-49_rough_rider_turnpike_loop', 
        #'2023-11-14-15-22-01_figure_8', 
        #'figure_8_2023-09-13-17-23-35', 
        #'figure_8_2023-09-13-17-24-26', 
        #'figure_8_2023-09-14-11-37-49', 
        #'figure_8_morning_2023-09-12-10-37-17', 
        #'figure_8_morning_slow_2023-09-12-11-06-32', 
        #'figure_8_to_turnpike_2023-09-12-12-22-45', 
        #'figure_8_to_turnpike_pt2_2023-09-12-12-28-48', 
        #'figure_8_turnpike_2023-09-13-17-40-27', 
        #'figure_8_turnpike_2023-09-13-17-47-52', 
        #'gupta_2023-09-14-10-46-16', 
        #'gupta_2023-09-14-10-47-09', 
        #'gupta_2023-09-14-11-12-19', 
        #'gupta_skydio_2023-09-14-11-03-09',
        #'gupta_warehouse_2023-09-13-19-12-49',
        #'meadows_2023-09-14-11-45-21',
        #'meadows_2023-09-14-12-07-28',
        #'meadows_2023-09-14-12-08-57',
        #'meadows_2023-09-14-12-17-25',
        #'meadows_2023-09-14-12-18-22',
        #'sam_loop_2023-09-27-12-42-17',
        #'sam_loop_2023-09-27-13-04-54',
        #'slag_heap_2023-09-14-12-23-05',
        #'slag_heap_skydio_2023-09-14-12-32-13',
        #'slag_heap_skydio_2023-09-14-12-36-46',
        #'turnpike_2023-09-12-12-39-19',
        #'turnpike_2023-09-12-12-42-49',
        #'turnpike_2023-09-12-12-47-34',
        #'turnpike_2023-09-12-12-53-32',
        #'turnpike_2023-09-13-17-59-09',
        #'turnpike_2023-09-13-18-09-45',
        #'turnpike_2023-09-13-18-15-03',
        #'turnpike_flat_2023-09-12-13-08-55',
        #'turnpike_flat_2023-09-12-13-11-44',
        #'turnpike_flat_2023-09-12-13-42-26',
        #'turnpike_flat_2023-09-12-14-16-06',
        #'turnpike_flat_2023-09-12-14-21-03',
        #'turnpike_skydio_2023-09-13-18-00-11',
        #'turnpike_skydio_2023-09-14-13-57-56',
        #'turnpike_to_gupta_flat_2023-09-12-14-31-21',
        #'turnpike_warehouse_2023-09-13-19-03-05',
        #'turnpike_warehouse_2023-09-14-14-05-14',
        #'turnpike_warehouse_2023-09-14-14-07-12',
        #'warehouse_gupta_2023-09-13-16-23-26',
        #'warehouse_gupta_2023-09-13-16-28-57',
        #'warehouse_to_fence_2023-09-12-10-19-17'
    ],
    'bags': [
        'tartandrive_v2/2022-05-06-16-03-24/kitti',
        'tartandrive_v2/2022-05-06-16-06-24/kitti',
        'tartandrive_v2/2022-05-06-16-09-24/kitti'
    ]
}

class Progress:
    def __init__(self, filename):
        self.pbar = None
        self.filename = filename

    def set_meta(self, object_name, total_length):
        self.pbar = tqdm(total=total_length, unit='B', unit_scale=True,
                        desc=f"Downloading {self.filename}")

    def update(self, size):
        if self.pbar:
            self.pbar.update(size)

    def done(self):
        if self.pbar:
            self.pbar.close()

def init_minio_client():
    """Initialize and return MinIO client"""
    return Minio(
        endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        secure=True,
        cert_check=False
    )

def get_full_item_name(item_name, data_type):
    """Add appropriate extension to item name based on data type"""
    if data_type == 'kitti' and not item_name.endswith(('.tar', '.txt')):
        return f"{item_name}.tar"
    return item_name

def expand_default_items(items, data_type, directory):
    """Expand default items to include .tar files and all files in subfolders"""
    expanded = []
    prefix_path = f"{data_type}/all_topics/{directory}/" if data_type == 'kitti' else f"bags/{directory}/"
    
    if prefix_path not in FILE_MAP[data_type]:
        return expanded

    for item in items:
        # For .tar files, append .tar extension
        tar_name = f"{prefix_path}{item}.tar"
        if 'files' in FILE_MAP[data_type][prefix_path] and tar_name in FILE_MAP[data_type][prefix_path]['files']:
            expanded.append(tar_name)
            
        # For subfolders, include all files in that folder
        subfolder = f"{prefix_path}{item}/"
        if item in FILE_MAP[data_type][prefix_path] and isinstance(FILE_MAP[data_type][prefix_path][item], dict):
            if 'files' in FILE_MAP[data_type][prefix_path][item]:
                expanded.extend(FILE_MAP[data_type][prefix_path][item]['files'])
    
    return expanded

def filter_items(items, items_to_download, data_type=None, directory=None):
    """Filter items based on requested items"""
    if not items_to_download:
        return items
        
    # If these are default items, expand them first
    if data_type and directory:
        items_to_download = expand_default_items(items_to_download, data_type, directory)
    
    # Now filter based on exact matches
    return [item for item in items if item in items_to_download]

def list_items(minio_client, bucket_name, directory, data_type):
    """List items in a directory, including items in subdirectories"""
    items = []
    try:
        if data_type == 'kitti':
            prefix_path = f"kitti/all_topics/{directory}/"
            if prefix_path in FILE_MAP[data_type]:
                # Add files from the main directory
                if 'files' in FILE_MAP[data_type][prefix_path]:
                    items.extend(FILE_MAP[data_type][prefix_path]['files'])
                
                # Check each key in the directory for subdirectories
                for key in FILE_MAP[data_type][prefix_path]:
                    if isinstance(FILE_MAP[data_type][prefix_path][key], dict):
                        # This is a subdirectory
                        if 'files' in FILE_MAP[data_type][prefix_path][key]:
                            items.extend(FILE_MAP[data_type][prefix_path][key]['files'])
        else:  # bags
            prefix_path = f"bags/{directory}/"
            if prefix_path in FILE_MAP[data_type]:
                items.extend(FILE_MAP[data_type][prefix_path].get('files', []))
    except Exception as e:
        print(f"Error listing items: {e}")
    return items

def get_all_possible_items():
    """Get all unique items available across all directories"""
    all_items = {}
    for data_type, type_dirs in DOWNLOAD_DIRS.items():
        all_items[data_type] = set()
        for directory in type_dirs:
            try:
                if data_type == 'kitti':
                    prefix_path = f"kitti/all_topics/{directory}/"
                    if prefix_path in FILE_MAP[data_type]:
                        # Add files from the main directory
                        if 'files' in FILE_MAP[data_type][prefix_path]:
                            all_items[data_type].update(FILE_MAP[data_type][prefix_path]['files'])
                        
                        # Check each key in the directory for subdirectories
                        for key in FILE_MAP[data_type][prefix_path]:
                            if isinstance(FILE_MAP[data_type][prefix_path][key], dict):
                                # This is a subdirectory
                                if 'files' in FILE_MAP[data_type][prefix_path][key]:
                                    all_items[data_type].update(FILE_MAP[data_type][prefix_path][key]['files'])
                else:  # bags
                    prefix_path = f"bags/{directory}/"
                    if prefix_path in FILE_MAP[data_type]:
                        all_items[data_type].update(FILE_MAP[data_type][prefix_path].get('files', []))
            except Exception as e:
                print(f"Warning: Error getting items from {directory}: {e}")
    return all_items

def get_available_items(data_type, directory):
    """Get available tar files (without extension) and subfolders for a directory"""
    tar_files = set()
    subfolders = set()
    
    prefix_path = f"{data_type}/all_topics/{directory}/" if data_type == 'kitti' else f"bags/{directory}/"
    if prefix_path in FILE_MAP[data_type]:
        dir_data = FILE_MAP[data_type][prefix_path]
        
        # Get .tar files from main directory
        if 'files' in dir_data:
            for item in dir_data['files']:
                if item.endswith('.tar'):
                    # Remove prefix path and .tar extension
                    base = os.path.basename(item)[:-4]
                    tar_files.add(base)
        
        # Get subfolders by looking for nested dictionaries with 'files'
        for key, value in dir_data.items():
            if isinstance(value, dict) and 'files' in value:
                # Just use the folder name without the full path
                folder_name = key.rstrip('/').split('/')[-1]
                subfolders.add(folder_name)
    
    return sorted(tar_files), sorted(subfolders)

def download_file(minio_client, bucket_name, object_name, save_path):
    """Download a single file from MinIO with progress bar"""
    try:
        # Create parent directory if it doesn't exist
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        # Create progress tracker
        progress = Progress(os.path.basename(object_name))

        # Download with progress
        minio_client.fget_object(bucket_name, object_name, save_path, progress=progress)
        return True
    except S3Error as e:
        print(f"Error downloading {object_name}: {e}")
        return False

def download_directory(minio_client, bucket_name, directory, save_path, data_type):
    """Download all files from a directory"""
    prefix_path = f"{data_type}/all_topics/{directory}/" if data_type == 'kitti' else f"bags/{directory}/"
    if prefix_path not in FILE_MAP[data_type]:
        print(f"Directory {directory} not found in {data_type}")
        return

    dir_data = FILE_MAP[data_type][prefix_path]
    
    # Download .tar files from DEFAULT_ITEMS
    if 'files' in dir_data:
        for item in dir_data['files']:
            basename = os.path.basename(item)
            name_without_ext = os.path.splitext(basename)[0]
            
            # For .tar files, check if base name is in DEFAULT_ITEMS
            if item.endswith('.tar') and name_without_ext in DEFAULT_ITEMS[data_type]:
                try:
                    # Create the full save path preserving the directory structure
                    rel_path = os.path.relpath(item, f"{data_type}/all_topics/{directory}" if data_type == 'kitti' else f"bags/{directory}")
                    full_save_path = os.path.join(save_path, directory, rel_path)
                    
                    # Create directory if it doesn't exist
                    os.makedirs(os.path.dirname(full_save_path), exist_ok=True)
                    
                    # Download the file using the full path from files.yaml
                    if download_file(minio_client, bucket_name, item, full_save_path):
                        print(f"Successfully downloaded {item}")
                except Exception as e:
                    print(f"Error downloading {item}: {e}")
    
    # Download files from subfolders that match DEFAULT_ITEMS
    for key, value in dir_data.items():
        if isinstance(value, dict) and 'files' in value:
            folder_name = key.rstrip('/').split('/')[-1]
            if folder_name in DEFAULT_ITEMS[data_type]:
                for item in value['files']:
                    try:
                        # Create the full save path preserving the directory structure
                        rel_path = os.path.relpath(item, f"{data_type}/all_topics/{directory}" if data_type == 'kitti' else f"bags/{directory}")
                        full_save_path = os.path.join(save_path, directory, rel_path)
                        
                        # Create directory if it doesn't exist
                        os.makedirs(os.path.dirname(full_save_path), exist_ok=True)
                        
                        # Download the file using the full path from files.yaml
                        if download_file(minio_client, bucket_name, item, full_save_path):
                            print(f"Successfully downloaded {item}")
                    except Exception as e:
                        print(f"Error downloading {item}: {e}")

def list_objects(minio_client, bucket_name, prefix=""):
    """List all objects in the bucket with given prefix"""
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
    return [obj.object_name for obj in objects]

def download_directories(minio_client, bucket_name, directories, base_save_path, data_type):
    """Download selected files from multiple directories"""
    for directory in directories:
        save_path = os.path.join(base_save_path, os.path.basename(directory))
        print(f"\nDownloading directory: {directory}")
        download_directory(minio_client, bucket_name, directory, save_path, data_type)

def main():
    parser = argparse.ArgumentParser(description="Download dataset from MinIO storage")
    parser.add_argument("--type", type=str, choices=['kitti', 'bags'],
                      help="Type of data to download (kitti or bags)")
    parser.add_argument("--save-path", type=str, required=True,
                      help="Path to save downloaded files")
    parser.add_argument("--all", action='store_true',
                      help="Download all directories")
    parser.add_argument("--list-only", action='store_true',
                      help="Only list available items without downloading")

    args = parser.parse_args()

    if args.list_only:
        print("\nAvailable items in all directories:")
        for data_type, directories in DOWNLOAD_DIRS.items():
            print(f"\n=== {data_type.upper()} ===")
            for directory in directories:
                print(f"\nDirectory: {directory}")
                tar_files, subfolders = get_available_items(data_type, directory)
                
                if tar_files:
                    print("  TAR files (add .tar when downloading):")
                    for item in tar_files:
                        print(f"    - {item}")
                
                if subfolders:
                    print("  Subfolders (all files will be downloaded):")
                    for folder in subfolders:
                        print(f"    - {folder}")
        return

    if not args.type:
        parser.error("--type is required unless --list-only is specified")

    directories = DOWNLOAD_DIRS[args.type]
    
    # Create base save directory if it doesn't exist
    os.makedirs(args.save_path, exist_ok=True)

    # Initialize MinIO client
    minio_client = init_minio_client()
    download_directories(minio_client, bucket_name, directories, args.save_path, args.type)

    print("\nDownload completed!")

if __name__ == "__main__":
    main()
