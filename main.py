from app import setup_web_server
from utils import config


def main():
    """ start the server """

    (setup_web_server()
     .run(host=config['server']['host'], port=config['server']['port']))


if __name__ == '__main__':
    main()
