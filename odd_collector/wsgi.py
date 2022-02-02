import logging
import os

from odd_models.adapter import init_flask_app, init_controller

from .cache import Cache
from .config import config
from .controllers import Controller
from .scheduler import Scheduler


from odd_collector.module_importer import get_config, load_plugins_packages


logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
)


def create_app(conf):
    collector_config = get_config(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "../collector_config.yaml"
        )
    )
    loaded_adapters = load_plugins_packages(collector_config)

    app = init_flask_app()
    app.config.from_object(conf)

    with app.app_context():
        for package, plugin_config in loaded_adapters:
            cache = Cache()
            adapter = package.adapter.Adapter(plugin_config)
            init_controller(Controller(adapter, cache))
            Scheduler(adapter, cache).start_scheduler(
                collector_config.default_pulling_interval
            )
        return app


application = create_app(config)
