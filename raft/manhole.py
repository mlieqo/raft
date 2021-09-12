from typing import Dict, Any

import os

import aiomanhole


def start_manhole(
    host: str, port: int, socket_path: str, namespace: Dict[str, Any]
) -> None:
    if socket_path is not None:
        try:
            os.unlink(socket_path)
        except FileNotFoundError:
            pass

    banner = '\n\t'.join(name for name in namespace.keys())

    aiomanhole.start_manhole(
        host=host,
        port=port,
        path=socket_path,
        namespace=namespace,
        banner=f'Namespace: \n\t{banner}\n\n',
    )
