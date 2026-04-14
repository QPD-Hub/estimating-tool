from wsgiref.simple_server import make_server

from src.config import AppConfig
from src.web import create_app


def main() -> None:
    config = AppConfig.load()
    app = create_app(config)

    with make_server("0.0.0.0", config.port, app) as server:
        print(
            f"Document handoff app running on http://0.0.0.0:{config.port} "
            f"({config.app_env})"
        )
        server.serve_forever()


if __name__ == "__main__":
    main()
