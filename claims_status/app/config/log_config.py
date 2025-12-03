import logging

from app.config.settings import get_settings


class Logger:
    """
    Logger utility for sending logs to console using StreamHandler.
    """

    def __init__(
        self,
        logger_name: str = "APILogs",
    ) -> None:
        self.settings = get_settings()
        self.logger_name = logger_name

    def get_logger(self) -> logging.Logger:
        """
        Returns a logger configured with StreamHandler for console output.
        """
        logger = logging.getLogger(self.logger_name)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        # Skip if handlers already exist
        if logger.handlers:
            return logger

        # Create formatter
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        return logger

logger = Logger().get_logger()
