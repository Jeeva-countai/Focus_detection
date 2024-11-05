import os
import datetime
import time
import psycopg2
import threading

class FetchImage:
    def __init__(self, db_connection):
        self.conn = db_connection  # Store the database connection
        self.roll_id = None
        self.roll_number = None
        self.roll_name = None
        self.revolution = None
        self.camera_name = None
        self.input_dir = ""
        self.previous_data = {
            "roll_id": None,
            "roll_number": None,
            "roll_name": None,
            "revolution": None
        }

    def fetch_roll_details(self):
        """Fetches roll details from roll_details table where roll_sts_id is 1."""
        query = """
        SELECT roll_id, roll_number, roll_name, revolution
        FROM roll_details
        WHERE roll_sts_id = 1
        """
        
        with self.conn.cursor() as cursor:  # Use the passed connection
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result:
                current_data = {
                    "roll_id": result[0],
                    "roll_number": result[1],
                    "roll_name": result[2],
                    "revolution": result[3]
                }
                
                # Compare each field with previous data to detect changes
                changes_detected = False
                if current_data["roll_id"] != self.previous_data["roll_id"]:
                    changes_detected = True
                if current_data["roll_number"] != self.previous_data["roll_number"]:
                    changes_detected = True
                if current_data["roll_name"] != self.previous_data["roll_name"]:
                    changes_detected = True
                if current_data["revolution"] != self.previous_data["revolution"]:
                    changes_detected = True
                
                # If changes detected, update previous data and store the fetched values
                if changes_detected:
                    self.previous_data = current_data
                    self.roll_id = current_data["roll_id"]
                    self.roll_number = current_data["roll_number"]
                    self.roll_name = current_data["roll_name"]
                    self.revolution = current_data["revolution"]
                    print(f"Fetched updated roll details: {current_data}")
                    return current_data  # Return the current data
                else:
                    print("No changes in roll details, skipping fetch.")
            else:
                print("No active roll details found.")
        return None  # Return None if no details found


    def fetch_cam_name(self):
        """Fetches the camera name from cam_details table where camsts_id is 1."""
        query = """
        SELECT cam_name
        FROM cam_details
        WHERE camsts_id::int = 1  -- Cast camsts_id to integer
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                self.camera_name = result[0]
                print(f"Fetched camera name: {self.camera_name}")
                return self.camera_name  # Return the camera name
            else:
                print("No active camera found.")
                return None


    def fetch_images_from_directory(self):
        """Fetches image paths from the specified input directory."""
        if not all([self.roll_id, self.camera_name]):
            print("Missing roll ID or camera name, unable to fetch images.")
            return []

        # Construct the directory path
        self.input_dir = f"/home/kniti/projects/knit-i/knitting-core/images/{self.roll_id}/{datetime.datetime.now().date()}/{self.camera_name}/cam1/{datetime.datetime.now().hour}/"
        
        print(f"Looking for images in: {self.input_dir}")  # Print the directory being checked
        
        if not os.path.exists(self.input_dir):
            print(f"No directory found at {self.input_dir}")
            return []
        
        # Fetch image file paths
        images = [os.path.join(self.input_dir, file) for file in os.listdir(self.input_dir) if file.endswith(".jpg")]
        print(f"Fetched {len(images)} images from {self.input_dir}")
        for img in images:
            print(f"Image path: {img}")  # Print each image path
        return images

    def monitor_roll_changes(self, active_camera_names):
        """Continuously monitors roll details for changes and triggers processing when needed."""
        updated_doff_list = []

        while True:
            roll_details = self.fetch_roll_details()
            self.fetch_cam_name()  # Ensure camera name is fetched every iteration

            # Ensure roll details are available before proceeding
            if roll_details is not None and self.camera_name:
                # Fetch images if valid details exist
                images = self.fetch_images_from_directory()
                if not images:
                    print("No images fetched, retrying...")
                    time.sleep(5)
                    continue
                
                current_roll_id = self.roll_id
                current_doff = self.revolution

                if self.previous_data["roll_id"] is None:
                    self.previous_data["roll_id"] = current_roll_id
                    continue

                if current_roll_id != self.previous_data["roll_id"] and len(updated_doff_list) > 1:
                    threading.Thread(
                        target=self.process_doff_list, 
                        args=(updated_doff_list, self.previous_data["roll_id"], active_camera_names)
                    ).start()
                    updated_doff_list = []
                    self.previous_data["roll_id"] = current_roll_id

                updated_doff_list.append(current_doff)

                if current_doff % 100 == 0 and len(updated_doff_list) > 1:
                    threading.Thread(
                        target=self.process_doff_list, 
                        args=(updated_doff_list, self.previous_data["roll_id"], active_camera_names)
                    ).start()
                    updated_doff_list = []

            else:
                print("No valid roll or camera details found. Retrying...")

            time.sleep(5)  # Adjust frequency of checking as needed


    def process_doff_list(self, updated_doff_list, roll_id, active_camera_names):
        """Processes the doff list when significant changes occur."""
        print(f"Processing doff list for roll ID {roll_id} with updated doff list {updated_doff_list}")

