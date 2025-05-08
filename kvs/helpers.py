import logging

def setup_logging(level: int = logging.DEBUG) -> None:
    """ Configure the logger. """

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
