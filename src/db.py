import traceback
import psycopg2
import datetime


class Database:
    """
    Class for handling database operations.
    """

    def __init__(self, database="knitting", user="postgres", password="55555", host="127.0.0.1", port="5432",
                 keepalive_kwargs=None):
        """
        Initialize the Database object with connection parameters.
        
        Args:
            database (str): The name of the database.
            user (str): The username for database authentication.
            password (str): The password for database authentication.
            host (str): The host address of the database server.
            port (str): The port number of the database server.
            keepalive_kwargs (dict): Optional dictionary of keepalive parameters for the database connection.
        """
        self.keepalive_kwargs = keepalive_kwargs or {
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 5,
            "keepalives_count": 5,
        }
        self.conn = self.connect(database, user, password, host, port)

    def connect(self, database, user, password, host, port):
        """
        Establish a connection to the PostgreSQL database.
        
        Args:
            database (str): The name of the database.
            user (str): The username for database authentication.
            password (str): The password for database authentication.
            host (str): The host address of the database server.
            port (str): The port number of the database server.
            
        Returns:
            psycopg2.connection: The connection object.
        """
        conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port,
            **self.keepalive_kwargs
        )
        conn.autocommit = True
        return conn

    def execute_query(self, query):
        """
        Execute a SQL query.
        
        Args:
            query (str): The SQL query to execute.
            
        Returns:
            bool: True if the query was executed successfully, False otherwise.
        """
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            cur.close()
            return True
        except Exception as e:
            print(str(e))
            return False

    def fetch_query(self, query):
        """
        Fetch results of a SQL query.
        
        Args:
            query (str): The SQL query to execute.
            
        Returns:
            list: A list of dictionaries representing the query results.
        """
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            rows = [
                dict((cur.description[i][0], value) for i, value in enumerate(row))
                for row in cur.fetchall()
            ]
            cur.close()
            return rows
        except Exception as e:
            print(str(e))
            return False


class StatusDB:
    """
    Class for managing status-related database operations.
    """

    def __init__(self):
        """
        Initialize the StatusDB object.
        """
        self.execute = Database()

    def get_uptime_status(self):
        """
        Retrieve uptime status records from the database.
        
        Returns:
            list: A list of uptime status records.
        """
        try:
            return self.execute.fetch_query("SELECT * FROM public.live_status")
        except Exception as e:
            print(str(e))
            return False

    def update_status(self, data):
        """
        Update status records in the database.
        
        Args:
            data (dict): Dictionary containing status data to update.
            
        Returns:
            bool: True if the update was successful, False otherwise.
        """
        try:
            cam_active_sts = {item['cam_name']: item['livecamsts_id'] for item in self.execute.fetch_query("SELECT cam_name,livecamsts_id,liveimgsts_id FROM public.cam_details ORDER BY cam_name ASC")}
            sql = (
                "INSERT INTO public.uptime_status ("
                "machine_status, software_status, controller_status, timestamp, image_status, "
                "ml_status, alarm_status, monitor_status, ui_status, report_status, redis_status, "
                '''"blackcam-wired_status", "blackcam-wireless_status", greencam1_status, greencam2_status, voltcam_status) VALUES ('''
                f"'{data[0]['machine_status']}', '{data[0]['software_status']}', '{data[0]['controller_status']}', "
                f"'{str(datetime.datetime.now())}', '{data[0]['image_status']}',"
                f"'{data[0]['ml_status']}', '{data[0]['alarm_status']}', '{data[0]['monitor_status']}', "
                f"'{data[0]['ui_status']}', '{data[0]['report_status']}', '{data[0]['redis_status']}',"
                f'\'{cam_active_sts["blackcam-wired"]}\', \'{cam_active_sts["blackcam-wireless"]}\', \'{cam_active_sts["greencam1"]}\', \'{cam_active_sts["greencam2"]}\', \'{cam_active_sts["voltcam"]}\')'
            )
            return self.execute.execute_query(sql)
        except Exception as e:
            print(str(e))
            return False

    def get_status_by_type(self, target_date, status_type):
        """
        Retrieve status records from the database by type and date.
        
        Args:
            target_date (str): The target date for the query.
            status_type (str): The type of status to retrieve.
            
        Returns:
            list: A list of status records.
        """
        try:
            sql = (
                f"SELECT timestamp FROM uptime_status WHERE DATE(timestamp) = '{str(target_date)}' "
                f"GROUP BY timestamp HAVING COUNT(CASE WHEN {status_type} = '1' THEN 1 ELSE NULL END) = 1;"
            )
            return self.execute.fetch_query(sql)
        except Exception as e:
            print(str(e))
            return False

    def reset_status(self):
        """
        Reset status records in the database.
        
        Returns:
            bool: True if the reset was successful, False otherwise.
        """
        try:
            sql = (
                "UPDATE public.live_status SET edgecam1_status = '0', edgecam2_status = '0', edgecam3_status = '0', "
                "machine_status = '0', controller_status = '0', software_status = '0', camera_status = '0', "
                "image_status = '0', ml_status = '0', alarm_status = '0', monitor_status = '0', "
                "ui_status = '0', report_status = '0', redis_status = '0' WHERE livestatus_id = 1"
            )
            return self.execute.execute_query(sql)
        except Exception as e:
            print(str(e))
            return False
        
    def get_active_cameras(self):
        """
        Retrieve active camera status records from the database.
        
        Returns:
            list: A list of active camera status records.
        """
        try:
            return  [item['cam_name'] for item in self.execute.fetch_query("SELECT cam_name FROM public.cam_details WHERE livecamsts_id = '1'")]
        except Exception as e:
            print(str(e))
            return False


class MainDB:
    def __init__(self):
        self.execute = Database()
    
    def storage_check(self,section,key):
        """
        Check storage space using AutoDelete objects.
        """
        try:
            query = f"SELECT {key} FROM public.storage_check where section = '{section}'"
            # print(query)
            return self.execute.fetch_query(query)
        except Exception as e:
            print(e)
            traceback_info = traceback.format_exc()
            print("Traceback:", traceback_info)

    def fetch_sentry_link(self):
        try:
            query = "SELECT sentry_link FROM public.monitor_config"
            print(self.execute.fetch_query(query))
            return self.execute.fetch_query(query)[0]['sentry_link']
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            
    def fetch_mail_list(self):
        try:
            query = "SELECT mail_list from public.monitor_config"
            print(self.execute.fetch_query(query))
            return self.execute.fetch_query(query)[0]['mail_list']
        except Exception as e:
            print(str(e))
            traceback.print_exc()

    def fetch_mail_time(self):
        try:
            query = "SELECT mail_time from public.monitor_config"
            print(self.execute.fetch_query(query))
            return self.execute.fetch_query(query)[0]['mail_time']
        except Exception as e:
            print(str(e))
            traceback.print_exc()

    def fetch_mail_subject(self):
        try:
            query = "SELECT mail_subject from public.monitor_config"
            print(self.execute.fetch_query(query))
            return self.execute.fetch_query(query)[0]['mail_subject']
        except Exception as e:
            print(str(e))
            traceback.print_exc()

    def fetch_mail_body(self):
        try:
            query = "SELECT mail_body from public.monitor_config"
            print(self.execute.fetch_query(query))
            return self.execute.fetch_query(query)[0]['mail_body']
        except Exception as e:
            print(str(e))
            traceback.print_exc()

    def fetch_alert_reciever(self):
        try:
            query = "SELECT alert_reciever from public.monitor_config"
            print(self.execute.fetch_query(query))
            return self.execute.fetch_query(query)[0]['alert_reciever']
        except Exception as e:
            print(str(e))
            traceback.print_exc()
    
    def fetch_alert_repeat_duration(self):
        try:
            query = "SELECT alert_repeat_duration from public.monitor_config"
            print(self.execute.fetch_query(query))
            return self.execute.fetch_query(query)[0]['alert_repeat_duration']
        except Exception as e:
            print(str(e))
            traceback.print_exc()

    def fetch_machine_name(self):
        try:
            query = "SELECT machinedtl_name FROM public.machine_details"
            print(query)
            print(self.execute.fetch_query(query))
            return self.execute.fetch_query(query)[0]['machinedtl_name']
        except Exception as e:
            print(str(e))
            traceback.print_exc()
    
    def fetch_mail_sts(self):
        try:
            query = "SELECT mail_sts FROM public.monitor_config"
            print(self.execute.fetch_query(query))
            return True if self.execute.fetch_query(query)[0]['mail_sts']  == "1" else False    
        except Exception as e:
            print(str(e))
            traceback.print_exc()

    def fetch_old_roll(self):
        try:
            query = "SELECT * FROM public.roll_details ORDER BY roll_id ASC LIMIT 1"
            # print(self.execute.fetch_query(query))
            return self.execute.fetch_query(query)[0]
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return False
        
    def fetch_rotation_data(self, roll_id):
        try:
            query = f"SELECT * FROM public.rotation_details WHERE roll_id = {roll_id}"
            # print(self.execute.fetch_query(query))
            return self.execute.fetch_query(query)
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return False
    
    def delete_corefpr_log(self, rotation_id):
        try:
            query = "DELETE FROM public.corefpr_log WHERE revolution_id = " + str(rotation_id)
            return self.execute.execute_query(query)
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return False
        
    def fetch_defect_details(self, roll_id):
        try:
            query = f"SELECT * FROM public.defect_details WHERE roll_id = {roll_id}"
            return self.execute.fetch_query(query)
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return False
        
    def fetch_alarm_data(self, roll_id, defect_id):
        try:
            query = f"SELECT * FROM public.alarm_status WHERE roll_id = {roll_id} AND defect_id = {defect_id}"
            return self.execute.fetch_query(query)
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return False
    
    def delete_combined_alarm_data(self, alarm_id):
        try:
            query = f"DELETE FROM public.combined_alarm_defect_details WHERE alarm_id = {alarm_id}"
            return self.execute.execute_query(query)
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return False
        
    def delete_alarm_data(self, roll_id, defect_id):
        try:
            query = f"DELETE FROM public.alarm_status WHERE roll_id = {roll_id} AND defect_id = {defect_id}"
            return self.execute.execute_query(query)
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return False
    
    def delete_defect_data(self, roll_id):
        try:
            query = f"DELETE FROM public.defect_details WHERE roll_id = {roll_id}"
            return self.execute.execute_query(query)
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return False
        
    def delete_rotation_data(self, roll_id):
        try:
            query = f"DELETE FROM public.rotation_details WHERE roll_id = {roll_id}"
            return self.execute.execute_query(query)
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return False
        
    def delete_roll_data(self, roll_id):
        try:
            query = f"DELETE FROM public.roll_details WHERE roll_id = {roll_id}"
            return self.execute.execute_query(query)
        except Exception as e:
            print(str(e))
            traceback.print_exc()
            return False
    