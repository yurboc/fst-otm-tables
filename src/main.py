from modules import table_converter
from modules import table_uploader
import json
import os
import pika

CRED_GOOGLE = "credentials/cred-google.json"
CRED_FTP = "credentials/cred-ftp.json"
OUT_DIR = "output"
CONFIG_FILE = "config.json"


# Load config from JSON
def load_config(config_file):
    with open(config_file, "r") as f:
        return json.load(f)


# Get output file path
def get_output_file_path(file_name):
    return os.path.join(OUT_DIR, file_name)


# Convert one table from Google Spreadsheet to JavaScript (JSON)
def convert_table(converter, table_params):
    print("Table name:     ", table_params["generator_name"])
    output_file = get_output_file_path(table_params["output_file"])
    converter.setSpreadsheetId(table_params["spreadsheetId"])
    converter.setSpreadsheetRange(table_params["range"])
    converter.readTable()
    converter.parseData(table_params["fields"])
    converter.saveToFile(output_file)
    print("Done table:     ", table_params["generator_name"])


# Upload generated JavaScript file to FTP
def upload_table(uploader, table_params):
    print("Uploading file: ", table_params["output_file"])
    uploader.upload(table_params, local_dir=OUT_DIR)
    print("Uploaded file:  ", table_params["output_file"])


# Handle new task
def on_new_task_message(ch, method, properties, body):
    # Decode incoming message
    print(f"Got new task...")
    try:
        msg = json.loads(body.decode())  # get job name from message
        msg_job = msg.get("job", "no_job")
    except:
        print("Error decoding incoming message: ", body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    # Get job name
    print(f"Prepare convertor for job '{msg_job}'...")
    converter = table_converter.TableConverter(CRED_GOOGLE)
    uploader = table_uploader.TableUploader(CRED_FTP)
    converter.auth()
    uploader.start()
    config = load_config(CONFIG_FILE)
    for table_params in config:
        if msg_job not in [table_params.get("generator_name"), "all"]:
            print(f"Skipping table {table_params['generator_name']}")
            continue
        print(f"Processing table {table_params['generator_name']}...")
        convert_table(converter, table_params)
        upload_table(uploader, table_params)
        print(f"Done table {table_params['generator_name']}!")
    uploader.quit()
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print("Done converting!")


# MAIN
def main():
    print(f"Starting 'FST-OTM table converter' worker (PID={os.getpid()})...")
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.queue_declare(queue="task_queue")
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="task_queue", on_message_callback=on_new_task_message)
    print("Worker started, waiting for messages...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()
    print("Finished 'FST-OTM table converter' worker!")


# Entry point
if __name__ == "__main__":
    main()
