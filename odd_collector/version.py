from odd_collector_sdk.logger import logger


def print_version():
    import subprocess

    try:
        logger.info(
            subprocess.run(["poetry", "version"], capture_output=True).stdout.decode()
        )
    except Exception as e:
        logger.error("Couldn't show version. {e}")
