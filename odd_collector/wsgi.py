import logging
import os

from odd_models.adapter import init_flask_app, init_controller

from .adapter import DynamoDBAdapter
from .cache import Cache
from .config import log_env_vars
from .controllers import Controller
from .scheduler import Scheduler
import module_importer


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
)

LIST_OF_ADAPTERS = list(module_importer.get_adapters())

def create_app(conf):
    app = init_flask_app()
    app.config.from_object(conf)
    log_env_vars(app.config)
    with app.app_context():
        for modules in LIST_OF_ADAPTERS:
            if "adapter" in modules.__file__:
                cache = Cache()
                adapter = modules.Adapters(app.config)
                init_controller(Controller(adapter, cache))
                Scheduler(adapter, cache).start_scheduler(int(app.config['SCHEDULER_INTERVAL_MINUTES']))
        return app


if os.environ.get('FLASK_ENVIRONMENT') == "production":
    application = create_app('odd_collector.config.ProductionConfig')
else:
    application = create_app('odd_collector.config.DevelopmentConfig')
