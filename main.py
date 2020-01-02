"""

Usage:
    main.py
    main.py [--h=<host>] [--p=<port] [--w=workers]

Options:
    --h=<str>  Host [default: 0.0.0.0]
    --p=<Int>  Port [default: 1234]
    --w=<Int>  Workers [default: 1]

"""

from app import setup_web_server
from docopt import docopt


if __name__ == '__main__':

    docs = docopt(__doc__)

    (setup_web_server(workers=int(docs['--w']))
        .run(host=docs['--h'], port=int(docs['--p']), debug=True))

