"""Entry script of the frozen ``futrisk.exe`` (PyInstaller, see ``futrisk.spec``).

Same target as the ``futrisk`` console script of pyproject.toml (``futrisk.main:main``).
A separate launcher, instead of freezing ``src/futrisk/main.py`` itself, keeps
``futrisk.main`` a normal package module (not a second copy running as ``__main__``).
``main()`` calls ``multiprocessing.freeze_support()`` first, so process-pool workers
(clearing-worker, backtests) re-entering the exe with ``--multiprocessing-fork`` run
their task and exit there.
"""

from futrisk.main import main

if __name__ == "__main__":
    main()
