import atexit

from kleentimer import kleentimer
from kleenlogger import kleenlogger, KleenLogger
from duplicationdrivetos3.database_service import database
from duplicationdrivetos3.drive_service import drive
from duplicationdrivetos3.s3_service import s3


@atexit.register
def on_crash():
    database.close_conn()


def parse_url(url: str):
    url_temp = url.replace('https://drive.google.com/drive/', '')
    if url_temp == "my-drive":
        return "root"
    else:
        url_temp = url_temp.replace('folders/', '')
        return url_temp


def prompt_user_for_info():
    resume = input("Last session crashed ? (yes/no) : ").lower()
    if resume == "yes":
        return "resume"
    database.create_table()
    drive_type = input("Drive / Teamdrive ? : ").lower()
    drive_url = parse_url(input("Drive URL : ").strip())
    aws_key_id = input("AWS key id : ")
    aws_key_secret = input("AWS key secret : ")
    bucket_name = input("Bucket name : ")
    user_input = {
        'type': drive_type,
        'url': drive_url,
        'awskey': aws_key_id,
        'awssecret': aws_key_secret,
        'bucket': bucket_name
    }
    database.inject_config(drive_type, drive_url, aws_key_id, aws_key_secret, bucket_name)
    return user_input


def main():
    resume = False
    config = prompt_user_for_info()
    if config == "resume":
        config = {}
        inline_conf = database.get_config()
        config['url'] = inline_conf[1]
        config['type'] = inline_conf[0]
        config['awskey'] = inline_conf[2]
        config['awssecret'] = inline_conf[3]
        config['bucket'] = inline_conf[4]
        resume = True
    kleenlogger.logger.info(
        'The script run with following parameters : resume={}, url={}, awskey={}, awsSecret={}, bucket={}'.format(
            str(resume),
            config.get('url'),
            config.get('awskey'),
            config.get('awssecret'),
            config.get('bucket')
        )
    )
    kleentimer.start_timer()
    s3.init_service(config.get('awskey'), config.get('awssecret'), config.get('bucket'))
    drive.init_service()
    drive.list_items(config.get('url'), None, resume, "")
    size_uploaded = database.get_upload_size()
    print(size_uploaded)
    kleenlogger.logger.info(size_uploaded)
    database.close_conn()


if __name__ == '__main__':
    kleenlogger.init_logger(
        "DuplicateToS3",
        KleenLogger.DEBUG,
        "[%(asctime)s][%(levelname)s] : [%(filename)s][%(funcName)s] : %(message)s",
        "%Y-%m-%d %H:%M:%S",
        "utf-8"
    )
    kleentimer.init_timer("The script has run for {hours}h {minutes}min and {secondes}sec")
    main()
    kleentimer.end_timer()
    output = kleentimer.elapsed_time()
    print(output)
    kleenlogger.logger.info(output)
    exit()
