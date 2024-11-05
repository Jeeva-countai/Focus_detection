import cv2
import numpy as np
import os
import time
from BlurDetector import BlurDetector
from fetch import FetchImage  # Import FetchImage for database operations
from tqdm import tqdm
from src.db import Database  # Import your Database class

class BlurMain:
    def __init__(self):
        # Initialize Database connection without a path
        self.database = Database()  # Create a Database instance
        self.fetcher = FetchImage(self.database.conn)  # Pass the database connection to FetchImage

    def run_blur_detection(self):
        """
        Continuously run blur detection by fetching images from the directory.
        """
        while True:
            # Fetch roll and camera details from the database
            roll_details = self.fetcher.fetch_roll_details()  # Gets roll_name, roll_number, revolution
            cam_name = self.fetcher.fetch_cam_name()
            
            print(roll_details)
            print("------------------------------------------") 
            print(cam_name)         # Gets cam_name

            if roll_details is None or cam_name is None:
                print("No valid roll or camera details found. Retrying...")
                time.sleep(5)  # Retry interval
                continue

            roll_id = roll_details['roll_name']  # Extract roll_id
            folder_path = self.fetcher.fetch_images_from_directory(roll_id, cam_name)  # Construct image directory

            if not folder_path:
                print("No valid directory found. Retrying...")
                time.sleep(5)  # Retry interval
                continue

            blur_detector = BlurDetector(
                downsampling_factor=4,
                num_scales=3,
                scale_start=2,
                entropy_filt_kernel_sze=7,
                sigma_s_RF_filter=15,
                sigma_r_RF_filter=0.25,
                num_iterations_RF_filter=3,
                show_progress=False,
            )

            blurriness_scores = []
            start_time = time.time()

            # Iterate over images in the dynamically fetched folder path
            for filename in tqdm(os.listdir(folder_path)):
                if filename.endswith(".jpg"):
                    img_path = os.path.join(folder_path, filename)
                    img = cv2.imread(img_path, 0)

                    map = blur_detector.detectBlur(img)
                    blur_map_normalized = (map - np.min(map)) / (np.max(map) - np.min(map))
                    sobelx = cv2.Sobel(blur_map_normalized, cv2.CV_64F, 1, 0, ksize=3)
                    sobely = cv2.Sobel(blur_map_normalized, cv2.CV_64F, 0, 1, ksize=3)
                    gradient_magnitude = np.sqrt(sobelx**2 + sobely**2)
                    blurriness_score = 1 / np.mean(gradient_magnitude)
                    blurriness_scores.append(blurriness_score)

                    print(f'Image: {filename}, Blurriness Score: {blurriness_score}')

            print(f"Time taken: {time.time() - start_time} seconds")

            # Calculate and log average blurriness score
            avg_blurriness = np.mean(blurriness_scores)
            if avg_blurriness > 175:
                result = f"The folder {folder_path} contains mostly blurry images with a blurriness score of {avg_blurriness}."
            else:
                result = f"The folder {folder_path} does not contain mostly blurry images, with a blurriness score of {avg_blurriness}."

            log_file_path = "/home/kniti/Documents/focus/Focus_detection/log/log.txt"  # Update this to the desired log path
            with open(log_file_path, "a") as log_file:
                log_file.write(result + "\n")

            print(result)
            time.sleep(10)  # Adjust sleep time as necessary


# Example usage
if __name__ == "__main__":
    blur_main = BlurMain()
    blur_main.run_blur_detection()
