from modules import table_converter
from modules import table_uploader
from logging import handlers
import logging
import json
import os
import pika

LOG_FILE = "log/converter.log"
LOG_FORMAT = (
    "%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - %(message)s"
    # altrnative format: "%(asctime)s - %(levelname)s - %(message)s"
)
CRED_GOOGLE = "credentials/cred-google.json"
CRED_FTP = "credentials/cred-ftp.json"
OUT_DIR = "output"
CONFIG_FILE = "config.json"


# Setup logging
logging.basicConfig(
    level=logging.INFO,  # (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),  # write logs to console
        logging.handlers.TimedRotatingFileHandler(  # write logs to file
            LOG_FILE, when="midnight", backupCount=14
        ),
    ],
)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("oauth2client").setLevel(logging.WARNING)
logging.getLogger("pika").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


# Load config from JSON
def load_config(config_file):
    with open(config_file, "r") as f:
        return json.load(f)


# Get output file path
def get_output_file_path(file_name):
    return os.path.join(OUT_DIR, file_name)


# Convert one table from Google Spreadsheet to JavaScript (JSON)
def convert_table(converter, table_params):
    logger.info(f"Table name: {table_params['generator_name']}")
    output_file = get_output_file_path(table_params["output_file"])
    converter.setSpreadsheetId(table_params["spreadsheetId"])
    converter.setSpreadsheetRange(table_params["range"])
    converter.readTable()
    converter.parseData(table_params["fields"])
    converter.saveToFile(output_file)
    logger.info(f"Done table: {table_params['generator_name']}")


# Upload generated JavaScript file to FTP
def upload_table(uploader, table_params):
    logger.info(f"Uploading file: {table_params['output_file']}")
    uploader.upload(table_params, local_dir=OUT_DIR)
    logger.info(f"Uploaded file: {table_params['output_file']}")


# Handle new task
def on_new_task_message(ch, method, properties, body):
    # Decode incoming message
    logger.info("Got new task...")
    try:
        msg = json.loads(body.decode())  # get job name from message
        msg_job = msg.get("job", "no_job")
        msg_uuid = msg.get("uuid", "no_uuid")
    except:
        logger.warning(
            f"Error decoding incoming message: {body.decode()}", exc_info=True
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    # Get job name
    logger.info(f"Prepare convertor for job '{msg_job}'...")
    converter = table_converter.TableConverter(CRED_GOOGLE)
    uploader = table_uploader.TableUploader(CRED_FTP)
    converter.auth()
    uploader.start()
    config = load_config(CONFIG_FILE)
    for table_params in config:
        if msg_job not in [table_params.get("generator_name"), "all"]:
            logger.info(f"Skipping table {table_params['generator_name']}")
            continue
        logger.info(f"Processing table {table_params['generator_name']}...")
        convert_table(converter, table_params)
        upload_table(uploader, table_params)
        logger.info(f"Done table {table_params['generator_name']}!")
        message_json = json.dumps(
            {
                "uuid": msg_uuid,
                "table": table_params["generator_name"],
                "result": "done",
            }
        )
        connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        channel = connection.channel()
        channel.queue_declare(queue="results_queue", durable=True)
        channel.basic_publish(
            exchange="",
            routing_key="results_queue",
            body=message_json,
        )
        connection.close()
        logger.info("Done publishing result!")
    uploader.quit()
    ch.basic_ack(delivery_tag=method.delivery_tag)
    logger.info("Done converting!")


# MAIN
def main():
    logger.info(f"Starting table converter with PID={os.getpid()}...")
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.queue_declare(queue="task_queue")
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="task_queue", on_message_callback=on_new_task_message)
    logger.info("Worker started, waiting for messages...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()
    logger.info("Finished 'FST-OTM table converter' worker!")


# Entry point
if __name__ == "__main__":
    main()
